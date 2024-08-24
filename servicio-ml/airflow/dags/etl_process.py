from airflow.decorators import dag, task
import datetime

markdown_text = """
### Proceso ETL para el dataset de lluvia en Australia.

Se simula extracción de datos a partir de una URL, se transforman los datos y se realiza el split del dataset para su posterior entrenamiento.
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'dagrun_timeout': datetime.timedelta(minutes=60),
}

@dag(
    dag_id="etl_process_rain_australia",
    description="Proceso ETL para la carga de datos de diferentes estaciones de Australia.",
    doc_md=markdown_text,
    tags=["ETL", "Rain in Australia", "Weather"],
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    start_date=datetime.datetime(2023, 8, 10),
)
def process_etl_weatherAUS():

    @task.virtualenv(
        task_id="extract_data",
        requirements=["awswrangler==3.6.0"],
        system_site_packages=True,
    )
    def extract_data():
        import awswrangler as wr
        import pandas as pd

        url = "https://raw.githubusercontent.com/diegosarina-ceia/AMq1/main/dataset/weatherAUS.csv"
        weather_df = pd.read_csv(url)
        
        # Save the dataframe as a CSV file to pass between tasks
        data_path = "s3://data/raw/weatherAUS.csv"
        wr.s3.to_csv(df=weather_df, path=data_path, index=False)

    @task.virtualenv(
        task_id="transform_data",
        requirements=["scikit-learn==1.2.2", "awswrangler==3.6.0"],
        system_site_packages=True,
    )
    def transform_data():
        import awswrangler as wr
        import pandas as pd
        import numpy as np
        from sklearn.impute import SimpleImputer
        import boto3
        import botocore.exceptions
        import mlflow
        import json
        import datetime

        data_path = "s3://data/raw/weatherAUS.csv"
        weather_original_df = wr.s3.read_csv(data_path)

        weather_df = weather_original_df.copy()

        def treat_outliers(df):
            for column in df.select_dtypes(include=['float64', 'int64']).columns:
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                if column == 'Rainfall':
                    upper_bound = 40

                df[column] = df[column].apply(
                    lambda x: lower_bound if x < lower_bound else (upper_bound if x > upper_bound else x)
                )
            return df

        weather_df = treat_outliers(weather_df)

        VARIABLE_SALIDA = 'RainTomorrow'
        weather_df = weather_df.dropna(subset=[VARIABLE_SALIDA])

        # Replace value 9 in Cloud9am and Cloud3pm columns with NaN
        weather_df['Cloud9am'] = weather_df['Cloud9am'].replace(9, np.nan)
        weather_df['Cloud3pm'] = weather_df['Cloud3pm'].replace(9, np.nan)

        weather_df["Date"] = pd.to_datetime(weather_df['Date'])
        weather_df['Year'] = weather_df['Date'].dt.year
        weather_df['Month'] = weather_df['Date'].dt.month
        weather_df['Day'] = weather_df['Date'].dt.day

        weather_df.drop(columns='Date',inplace=True)

        weather_df["RainToday"] = weather_df["RainToday"].apply(lambda x: 1 if x == "Yes" else 0)
        weather_df["RainTomorrow"] = weather_df["RainTomorrow"].apply(lambda x: 1 if x == "Yes" else 0)

        categorical_cols = weather_df.select_dtypes(include=['object']).columns.tolist()

        # Handle missing values in categorical columns
        region_montly_mode = weather_df.groupby(['Month', 'Location'])[categorical_cols].agg(
            lambda x: x.mode().iloc[0] if not x.mode().empty else None
        )

        weather_df[categorical_cols] = weather_df.apply(
            lambda row: pd.Series(
                [region_montly_mode.loc[(row['Month'], row['Location']), col] if pd.isna(row[col]) else row[col] for col in categorical_cols],
                index=categorical_cols
            ),
            axis=1
        )

        monthly_mode_windgustdir = weather_df.groupby('Month')['WindGustDir'].agg(
            lambda x: x.mode().iloc[0] if not x.mode().empty else None
        )

        def fill_missing_windgustdir(row):
            locations = ["Newcastle", "Albany"]
            if row['Location'] in locations and pd.isna(row['WindGustDir']):
                return monthly_mode_windgustdir.loc[row['Month']]
            else:
                return row['WindGustDir']

        weather_df['WindGustDir'] = weather_df.apply(fill_missing_windgustdir, axis=1)

        # Create latitude and longitude columns based on location
        coordenadas = {
            "Albury": (-36.0737, 146.9135),
            "BadgerysCreek": (-33.9209, 150.7738),
            "Cobar": (-31.4996, 145.8380),
            "CoffsHarbour": (-30.2986, 153.1094),
            "Moree": (-29.4639, 149.8456),
            "Newcastle": (-32.9283, 151.7817),
            "NorahHead": (-33.2820, 151.5676),
            "NorfolkIsland": (-29.0408, 167.9547),
            "Penrith": (-33.7510, 150.7039),
            "Richmond": (-33.6000, 150.7760),
            "Sydney": (-33.8688, 151.2093),
            "SydneyAirport": (-33.9399, 151.1753),
            "WaggaWagga": (-35.1189, 147.3699),
            "Williamtown": (-32.7942, 151.8345),
            "Wollongong": (-34.4278, 150.8931),
            "Canberra": (-35.2809, 149.1300),
            "Tuggeranong": (-35.4150, 149.0670),
            "MountGinini": (-35.5292, 148.7723),
            "Ballarat": (-37.5622, 143.8503),
            "Bendigo": (-36.7570, 144.2794),
            "Sale": (-38.1065, 147.0733),
            "MelbourneAirport": (-37.6690, 144.8410),
            "Melbourne": (-37.8136, 144.9631),
            "Mildura": (-34.2086, 142.1310),
            "Nhil": (-36.3333, 141.6500),
            "Portland": (-38.3478, 141.6051),
            "Watsonia": (-37.7016, 145.0800),
            "Dartmoor": (-38.0806, 141.2714),
            "Brisbane": (-27.4698, 153.0251),
            "Cairns": (-16.9186, 145.7781),
            "GoldCoast": (-28.0167, 153.4000),
            "Townsville": (-19.2590, 146.8169),
            "Adelaide": (-34.9285, 138.6007),
            "MountGambier": (-37.8310, 140.7796),
            "Nuriootpa": (-34.4700, 138.9967),
            "Woomera": (-31.1989, 136.8255),
            "Albany": (-35.0275, 117.8847),
            "Witchcliffe": (-34.0150, 115.1000),
            "PearceRAAF": (-31.6671, 116.0147),
            "PerthAirport": (-31.9403, 115.9667),
            "Perth": (-31.9505, 115.8605),
            "SalmonGums": (-32.9735, 121.6365),
            "Walpole": (-34.9784, 116.7331),
            "Hobart": (-42.8821, 147.3272),
            "Launceston": (-41.4298, 147.1576),
            "AliceSprings": (-23.6980, 133.8807),
            "Darwin": (-12.4634, 130.8456),
            "Katherine": (-14.4650, 132.2635),
            "Uluru": (-25.3444, 131.0369)
        }

        weather_df['Latitude'] = weather_df['Location'].map(lambda loc: coordenadas.get(loc, (np.nan, np.nan))[0])
        weather_df['Longitude'] = weather_df['Location'].map(lambda loc: coordenadas.get(loc, (np.nan, np.nan))[1])

        weather_df.drop(columns='Location', inplace=True)

        # Encode wind direction as sine and cosine components
        def encode_wind_dir(df, col, mapping):
            angles = df[col].map(mapping)
            angles_rad = np.deg2rad(angles)
            df[f'{col}_sin'] = np.sin(angles_rad)
            df[f'{col}_cos'] = np.cos(angles_rad)
            df.loc[df[col].isna(), [f'{col}_sin', f'{col}_cos']] = np.nan
            df.drop(columns=col, inplace=True)

        direccion_to_angulo = {
            'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5, 'E': 90, 'ESE': 112.5,
            'SE': 135, 'SSE': 157.5, 'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
            'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
        }

        encode_wind_dir(weather_df, 'WindGustDir', direccion_to_angulo)
        encode_wind_dir(weather_df, 'WindDir9am', direccion_to_angulo)
        encode_wind_dir(weather_df, 'WindDir3pm', direccion_to_angulo)

        IMP_CORRECTA = ['MinTemp', 'MaxTemp', 'Rainfall', 'WindGustSpeed', 'WindSpeed9am', 'WindSpeed3pm', 'Humidity9am', 'Humidity3pm', 'Temp9am', 'Temp3pm']
        IMP_INCORRECTA = ['Evaporation', 'Sunshine', 'Pressure9am', 'Pressure3pm', 'Cloud9am', 'Cloud3pm']

        NUMERICAL_COLS = weather_df.select_dtypes(include=['float64']).columns.tolist()

        # Imputación con KNNImputer (se comenta a modo de prueba para evitar demoras)
        #scaler = StandardScaler()
        #scaled_features = scaler.fit_transform(weather_df[NUMERICAL_COLS])
        
        #knn_imputer = KNNImputer(n_neighbors=5)
        #imputed_knn = knn_imputer.fit_transform(scaled_features)
        #weather_df[NUMERICAL_COLS] = scaler.inverse_transform(imputed_knn)

        imputer = SimpleImputer(strategy='median')
        weather_df[NUMERICAL_COLS] = imputer.fit_transform(weather_df[NUMERICAL_COLS])

        data_end_path = "s3://data/raw/weatherAUS_corregido.csv"
        wr.s3.to_csv(df=weather_df, path=data_end_path, index=False)

        # Save metadata to S3
        client = boto3.client('s3')

        data_dict = {}
        try:
            client.head_object(Bucket='data', Key='data_info/data.json')
            result = client.get_object(Bucket='data', Key='data_info/data.json')
            data_dict = json.loads(result["Body"].read().decode())
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                data_dict = {}
            else:
                raise e

        # Upload JSON String to an S3 Object
        data_dict['columns'] = weather_original_df.columns.to_list()
        data_dict['columns_after_transform'] = weather_df.columns.to_list()
        data_dict['target_col'] = VARIABLE_SALIDA
        data_dict['categorical_columns'] = categorical_cols
        data_dict['columns_dtypes'] = {k: str(v) for k, v in weather_original_df.dtypes.to_dict().items()}
        data_dict['columns_dtypes_after_transform'] = {k: str(v) for k, v in weather_df.dtypes.to_dict().items()}
        category_dummies_dict = {}
        for category in categorical_cols:
            category_dummies_dict[category] = weather_original_df[category].unique().tolist()

        data_dict['categories_values_per_categorical'] = category_dummies_dict

        data_dict['date'] = datetime.datetime.today().strftime('%Y/%m/%d-%H:%M:%S"')
        data_string = json.dumps(data_dict, indent=2)
        client.put_object(Bucket='data', Key='data_info/data.json', Body=data_string)

        # Se registra el experimento en MLflow
        mlflow.set_tracking_uri('http://mlflow:5000')
        experiment = mlflow.set_experiment("Rain in Australia")
        mlflow.start_run(run_name='ETL_run_' + datetime.datetime.today().strftime('%Y/%m/%d-%H:%M:%S"'),
                        experiment_id=experiment.experiment_id,
                        tags={"experiment": "etl", "dataset": "Rain in Australia"})
        mlflow_dataset = mlflow.data.from_pandas(weather_original_df.sample(10),
                                                source="https://www.kaggle.com/datasets/jsphyg/weather-dataset-rattle-package",
                                                targets=VARIABLE_SALIDA,
                                                name="weather_data_complete")
        mlflow_dataset_transformed = mlflow.data.from_pandas(weather_df.sample(10),
                                                        source="https://www.kaggle.com/datasets/jsphyg/weather-dataset-rattle-package",
                                                        targets=VARIABLE_SALIDA,
                                                        name="weather_data_transformed")
        mlflow.log_input(mlflow_dataset, context="Dataset")                        
        mlflow.log_input(mlflow_dataset_transformed, context="Dataset")

    @task.virtualenv(
        task_id="split_dataset",
        requirements=["awswrangler==3.6.0", "scikit-learn"],
        system_site_packages=True
    )
    def split_dataset():
        import awswrangler as wr
        import pandas as pd
        from sklearn.model_selection import train_test_split

        data_transformed_path = "s3://data/raw/weatherAUS_corregido.csv"
        weather_df = wr.s3.read_csv(data_transformed_path)

        # Se separa el target del resto de las features
        X = weather_df.drop(columns='RainTomorrow')
        y = weather_df['RainTomorrow']

        # Se separa el conjunto de entrenamiento y testeo en 75% y 25% respectivamente.
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, random_state=1234)

        def save_to_csv(df, path):
            wr.s3.to_csv(df=df, path=path, index=False)

        save_to_csv(X_train, "s3://data/final/train/weather_X_train.csv")
        save_to_csv(X_test, "s3://data/final/test/weather_X_test.csv")
        save_to_csv(y_train, "s3://data/final/train/weather_y_train.csv")
        save_to_csv(y_test, "s3://data/final/test/weather_y_test.csv")

    @task.virtualenv(
        task_id="normalize_data",
        requirements=["awswrangler==3.6.0", "scikit-learn"],
        system_site_packages=True
    )
    def normalize_data():

        import json
        import mlflow
        import boto3
        import botocore.exceptions
        import awswrangler as wr
        import pandas as pd
        from sklearn.preprocessing import StandardScaler

        def save_to_csv(df, path):
            wr.s3.to_csv(df=df,
                         path=path,
                         index=False)

        X_train = wr.s3.read_csv("s3://data/final/train/weather_X_train.csv")
        X_test = wr.s3.read_csv("s3://data/final/test/weather_X_test.csv")

        sc_X = StandardScaler(with_mean=True, with_std=True)
        X_train_arr = sc_X.fit_transform(X_train)
        X_test_arr = sc_X.transform(X_test)

        X_train = pd.DataFrame(X_train_arr, columns=X_train.columns)
        X_test = pd.DataFrame(X_test_arr, columns=X_test.columns)

        save_to_csv(X_train, "s3://data/final/train/weather_X_train.csv")
        save_to_csv(X_test, "s3://data/final/test/weather_X_test.csv")

        # Save information of the dataset
        client = boto3.client('s3')

        try:
            client.head_object(Bucket='data', Key='data_info/data.json')
            result = client.get_object(Bucket='data', Key='data_info/data.json')
            text = result["Body"].read().decode()
            data_dict = json.loads(text)
        except botocore.exceptions.ClientError as e:
                # Something else has gone wrong.
                raise e

        # Upload JSON String to an S3 Object
        data_dict['standard_scaler_mean'] = sc_X.mean_.tolist()
        data_dict['standard_scaler_std'] = sc_X.scale_.tolist()
        data_string = json.dumps(data_dict, indent=2)

        client.put_object(
            Bucket='data',
            Key='data_info/data.json',
            Body=data_string
        )

        mlflow.set_tracking_uri('http://mlflow:5000')
        experiment = mlflow.set_experiment("Rain in Australia")

        # Obtain the last experiment run_id to log the new information
        list_run = mlflow.search_runs([experiment.experiment_id], output_format="list")

        with mlflow.start_run(run_id=list_run[0].info.run_id):

            mlflow.log_param("Train observations", X_train.shape[0])
            mlflow.log_param("Test observations", X_test.shape[0])
            mlflow.log_param("Standard Scaler feature names", sc_X.feature_names_in_)
            mlflow.log_param("Standard Scaler mean values", sc_X.mean_)
            mlflow.log_param("Standard Scaler scale values", sc_X.scale_)
        
    
    # Se definen las dependencias
    extract_data() >> transform_data() >> split_dataset() >> normalize_data()

dag = process_etl_weatherAUS()
