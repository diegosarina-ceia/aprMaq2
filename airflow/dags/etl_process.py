from airflow.decorators import dag, task
import datetime

markdown_text = """
### Proceso ETL para el dataset de lluvia en Australia.

Se simula extracción de datos a partir de una URL, se realiza limpieza de datos y se guarda el resultado.

"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'dagrun_timeout': datetime.timedelta(minutes=15)
}

@dag(
    dag_id="process_etl_weatherAUS",
    description="Proceso ETL para la carga de datos de lluvia en Australia.",
    doc_md=markdown_text,
    tags=["ETL", "Weather Australia"],
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    start_date=datetime.datetime(2023, 8, 10)
)
def process_etl_weatherAUS():

    @task.virtualenv(
        task_id="extract_data",
        requirements=["pandas", "awswrangler==3.6.0"],
        system_site_packages=False
    )
    def extract_data():
        import awswrangler as wr
        import pandas as pd

        url = "https://raw.githubusercontent.com/diegosarina-ceia/AMq1/main/dataset/weatherAUS.csv"
        weather_df = pd.read_csv(url)
        
        # Guardar el DataFrame como un archivo CSV para pasarlo entre tareas

        data_path = "s3://data/raw/weatherAUS.csv"
        wr.s3.to_csv(df=weather_df, path=data_path, index=False)
        
        return data_path

    @task.virtualenv(
        task_id="transform_data",
        requirements=["pandas", "numpy", "scikit-learn", "awswrangler==3.6.0"],
        system_site_packages=False
    )
    def transform_data(data_path):
        import awswrangler as wr
        import pandas as pd
        import numpy as np
        from sklearn.preprocessing import StandardScaler
        from sklearn.impute import KNNImputer
        import logging

        logger = logging.getLogger("airflow.task")

        weather_analisis_df = wr.s3.read_csv(data_path)
        logger.info(f"Dataset leído desde: {data_path}")

        # Función para detectar y tratar outliers usando IQR
        def tratar_outliers(df):
            for column in df.select_dtypes(include=['float64', 'int64']).columns:
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                if column == 'Rainfall':
                    upper_bound = 40

                #cantidad de valores fuera de los límites
                outliers = len(df[(df[column] < lower_bound) | (df[column] > upper_bound)])

                print(f"Columna: {column} - Límite inferior: {lower_bound} - Límite superior: {upper_bound} - Cantidad de outliers: {outliers}")
                
                # Reemplazar outliers con NaN (o podrías eliminarlos)
                df[column] = df[column].apply(lambda x: lower_bound if x < lower_bound else (upper_bound if x > upper_bound else x))
            
            return df

        weather_analisis_df = tratar_outliers(weather_analisis_df)
        logger.debug(f"Finalizó tratamiento de outliers")


        #carga de los datos
        weather_sin_outliers_df = weather_analisis_df.copy()

        VARIABLE_SALIDA = 'RainTomorrow'

        #aliminar los registros para los cuales la columna RainTomorrow tiene valores faltantes
        weather_sin_outliers_df = weather_sin_outliers_df.dropna(subset=[VARIABLE_SALIDA])

        # modificar el valor 9 en las columnas Cloud9am y Cloud3pm por NaN
        weather_sin_outliers_df['Cloud9am'] = weather_sin_outliers_df['Cloud9am'].replace(9, np.nan)
        weather_sin_outliers_df['Cloud3pm'] = weather_sin_outliers_df['Cloud3pm'].replace(9, np.nan)

        weather_sin_errores_df = weather_sin_outliers_df.copy(deep=True)

        # Creación de la variable Month
        weather_sin_errores_df["Date"] = pd.to_datetime(weather_sin_errores_df['Date'])
        weather_sin_errores_df['Month'] = weather_sin_errores_df['Date'].dt.month

        categorical_cols = weather_sin_errores_df.select_dtypes(include=['object']).columns.tolist()


        #.iloc[0] selecciona el primer valor del modal
        region_montly_mode = weather_sin_errores_df.groupby(['Month', 'Location'])[categorical_cols].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)

        # Rellenar valores faltantes para columnas categóricas
        weather_sin_errores_df[categorical_cols] = weather_sin_errores_df.apply(
                lambda row: pd.Series(
                    [region_montly_mode.loc[(row['Month'], row['Location']), col] if pd.isna(row[col]) else row[col] for col in categorical_cols],
                    index=categorical_cols
                ),
                axis=1
            )

        monthly_mode_windgustdir = weather_sin_errores_df.groupby('Month')['WindGustDir'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)

        def fill_missing_windgustdir_specific_locations(row):
            locations = ["Newcastle", "Albany"]
            if row['Location'] in locations and pd.isna(row['WindGustDir']):
                return monthly_mode_windgustdir.loc[row['Month']]
            else:
                return row['WindGustDir']

        weather_sin_errores_df['WindGustDir'] = weather_sin_errores_df.apply(fill_missing_windgustdir_specific_locations, axis=1)

        logger.debug(f"Finalizó tratamiento de datos faltantes")

        weather_impmed_df = weather_sin_errores_df.copy(deep=True)

        weather_impmed_df["Date"] = pd.to_datetime(weather_impmed_df['Date'])
        weather_impmed_df['Year'] = weather_impmed_df['Date'].dt.year
        weather_impmed_df['Month'] = weather_impmed_df['Date'].dt.month
        weather_impmed_df['Day'] = weather_impmed_df['Date'].dt.day

        weather_impmed_df.drop(columns='Date',inplace=True)

        weather_impmed_df["RainToday"] = weather_impmed_df["RainToday"].apply(lambda x: 1 if x == "Yes" else 0)
        weather_impmed_df["RainTomorrow"] = weather_impmed_df["RainTomorrow"].apply(lambda x: 1 if x == "Yes" else 0)

        #diccionario de coordenadas para las locaciones
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

        # Crear nuevas columnas para latitud y longitud
        weather_impmed_df['Latitude'] = None
        weather_impmed_df['Longitude'] = None

        # Asignar las coordenadas a cada ubicación en el DataFrame
        for index, row in weather_impmed_df.iterrows():
            location = row['Location']
            if location in coordenadas:
                weather_impmed_df.at[index, 'Latitude'] = float(coordenadas[location][0])
                weather_impmed_df.at[index, 'Longitude'] = float(coordenadas[location][1])

        #convertir columnas Latitude y Longitude a tipo float
        weather_impmed_df['Latitude'] = weather_impmed_df['Latitude'].astype(float)
        weather_impmed_df['Longitude'] = weather_impmed_df['Longitude'].astype(float)

        #eliminar columna Location
        weather_impmed_df.drop(columns='Location',inplace=True)

        direccion_to_angulo = {
            'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5, 'E': 90, 'ESE': 112.5,
            'SE': 135, 'SSE': 157.5, 'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
            'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
        }

        def codificacion_wind_dir(df, columna, direccion_to_angulo):
            # Se obtienen los ángulos a partir del mapeo
            angulos = df[columna].map(direccion_to_angulo)

            # Se convierten los ángulos a radianes
            angulos_rad = np.deg2rad(angulos)

            # Se crean las nuevas columnas con el seno y el coseno
            df[f'{columna}_sin'] = np.sin(angulos_rad)
            df[f'{columna}_cos'] = np.cos(angulos_rad)

            # Se setea NaN en las nuevas columnas para aquellas direcciones que sean nulas.
            df.loc[df[columna].isna(), [f'{columna}_sin', f'{columna}_cos']] = np.nan

            # Se elimina la columna original
            del df[columna]
            #return df

        codificacion_wind_dir(weather_impmed_df, 'WindGustDir', direccion_to_angulo)
        codificacion_wind_dir(weather_impmed_df, 'WindDir9am', direccion_to_angulo)
        codificacion_wind_dir(weather_impmed_df, 'WindDir3pm', direccion_to_angulo)

        weather_impmed_df2 = weather_sin_errores_df.copy(deep=True)

        #obtener columnas numericas en una lista
        numerical_cols = weather_impmed_df2.select_dtypes(include=['float64']).columns.tolist()

        #calculo la media de cada variable por mes y por region
        region_montly_median = weather_impmed_df2.groupby(['Month', 'Location'])[numerical_cols].median()

        region_montly_median.fillna(region_montly_median.median(), inplace=True)

        # Rellenar valores faltantes para columnas numericas
        weather_impmed_df2[numerical_cols] = weather_impmed_df2.apply(
            lambda row: pd.Series(
                [region_montly_median.loc[(row['Month'], row['Location']), col] if pd.isna(row[col]) else row[col] for col in numerical_cols],
                index=numerical_cols
            ),
            axis=1
        )

        IMP_CORRECTA = ['MinTemp', 'MaxTemp', 'Rainfall', 'WindGustSpeed', 'WindSpeed9am', 'WindSpeed3pm', 'Humidity9am', 'Humidity3pm', 'Temp9am', 'Temp3pm']
        IMP_INCORRECTA = ['Evaporation', 'Sunshine', 'Pressure9am', 'Pressure3pm', 'Cloud9am', 'Cloud3pm']

        #Armado de dataset con varibles correctamente imputadas.
        weather_imp_cod_df = weather_impmed_df.copy(deep=True)
        weather_imp_cod_df[IMP_CORRECTA]=weather_impmed_df2[IMP_CORRECTA]

        logger.debug(f"Finalizó imputación 1")

        weather_input_avanzada_df = weather_sin_errores_df.copy(deep=True)
        weather_input_avanzada_df[IMP_CORRECTA] = weather_impmed_df2[IMP_CORRECTA]

        #lista de variables numericas
        numerical_cols = weather_input_avanzada_df.select_dtypes(include=['float64']).columns.tolist()

        logger.debug(f"Comienza imputación con KNN...")
        # Imputación con KNNImputer
        #scaler = StandardScaler()
        #scaled_features = scaler.fit_transform(weather_input_avanzada_df[numerical_cols])
        
        #knn_imputer = KNNImputer(n_neighbors=5)
        #imputed_knn = knn_imputer.fit_transform(scaled_features)
        #weather_input_avanzada_df[numerical_cols] = scaler.inverse_transform(imputed_knn)
        #logger.debug(f"Finaliza imputación con KNN")

        weather_imp_cod_final_df = weather_impmed_df.copy(deep=True)
        weather_imp_cod_final_df[IMP_CORRECTA] = weather_impmed_df2[IMP_CORRECTA]
        weather_imp_cod_final_df[IMP_INCORRECTA] = weather_input_avanzada_df[IMP_INCORRECTA]

        data_end_path = "s3://data/raw/weatherAUS_corregido.csv"
        wr.s3.to_csv(df=weather_imp_cod_final_df, path=data_end_path, index=False)
        logger.debug(f"Finaliza transformación de datos, se almacena en {data_end_path}")
        
        return data_end_path

    @task.virtualenv(
        task_id="split_dataset",
        requirements=["awswrangler==3.6.0", "pandas", "scikit-learn"],
        system_site_packages=False
    )
    def split_dataset(data_transformed_path):
        import awswrangler as wr
        import pandas as pd
        from sklearn.model_selection import train_test_split

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
    
    # Define the task dependencies
    temp_path = extract_data()
    transformed_temp_path = transform_data(temp_path)
    split_dataset(transformed_temp_path)

dag = process_etl_weatherAUS()
