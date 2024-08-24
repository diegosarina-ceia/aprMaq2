import json
import pickle
import numpy as np
import pandas as pd
import boto3
import mlflow

from typing import Literal
from fastapi import FastAPI, Body, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field
from typing_extensions import Annotated


# Función para cargar el modelo (puedes ajustarla según tu flujo de trabajo actual)
def load_model(model_name: str, alias: str):
    try:
        # Cargar el modelo desde MLflow o localmente
        mlflow.set_tracking_uri('http://mlflow:5000')
        client_mlflow = mlflow.MlflowClient()

        model_data_mlflow = client_mlflow.get_model_version_by_alias(model_name, alias)
        model_ml = mlflow.sklearn.load_model(model_data_mlflow.source)
        version_model_ml = int(model_data_mlflow.version)
    except:
        # Cargar el modelo localmente si MLflow falla
        with open('/app/files/model.pkl', 'rb') as file_ml:
            model_ml = pickle.load(file_ml)
        version_model_ml = 0

    return model_ml, version_model_ml

# Clase de entrada basada en los 29 features del dataset con validaciones y descripciones
# Los features con alta correlacion se configuraron como opcionales
class ModelInput(BaseModel):
    MinTemp: float = Field(
        description="Minimum temperature of the day", ge=-50, le=60
    )
    MaxTemp: float = Field(
        description="Maximum temperature of the day", ge=-50, le=60
    )
    Rainfall: float = Field(
        description="Amount of rainfall in mm", ge=0, le=500
    )
    Evaporation: float = Field(
        description="Evaporation in mm", ge=0, le=50
    )
    Sunshine: float = Field(
        description="Sunshine duration in hours", ge=0, le=15
    )
    WindGustSpeed: float = Field(
        description="Maximum wind gust speed in km/h", ge=0, le=200
    )
    WindSpeed9am: float = Field(
        description="Wind speed at 9am in km/h", ge=0, le=150
    )
    WindSpeed3pm: float = Field(
        description="Wind speed at 3pm in km/h", ge=0, le=150
    )
    Humidity9am: float = Field(
        description="Humidity at 9am in percentage", ge=0, le=100
    )
    Humidity3pm: float = Field(
        description="Humidity at 3pm in percentage", ge=0, le=100
    )
    Pressure9am: Optional[float] = Field(
        default=None,
        description="Pressure at 9am in hPa", ge=0, le=1100
    )
    Pressure3pm: float = Field(
        description="Pressure at 3pm in hPa", ge=0, le=1100
    )
    Cloud9am: float = Field(
        description="Cloud cover at 9am on a scale of 0-9", ge=0, le=9
    )
    Cloud3pm: float = Field(
        description="Cloud cover at 3pm on a scale of 0-9", ge=0, le=9
    )
    Temp9am: Optional[float] = Field(
        default=None,
        description="Temperature at 9am in Celsius", ge=-50, le=60
    )
    Temp3pm: Optional[float] = Field(
        default=None,
        description="Temperature at 3pm in Celsius", ge=-50, le=60
    )
    RainToday: int = Field(
        description="Whether it rained today: 1 for Yes, 0 for No", ge=0, le=1
    )
    Month: int = Field(
        description="Month of the observation", ge=1, le=12
    )
    Year: int = Field(
        description="Year of the observation", ge=1900, le=2100
    )
    Day: int = Field(
        description="Day of the observation", ge=1, le=31
    )
    Latitude: float = Field(
        description="Latitude of the location", ge=-90, le=90
    )
    Longitude: float = Field(
        description="Longitude of the location", ge=-180, le=180
    )
    WindGustDir_sin: float = Field(
        description="Sine of the wind gust direction", ge=-1, le=1
    )
    WindGustDir_cos: float = Field(
        description="Cosine of the wind gust direction", ge=-1, le=1
    )
    WindDir9am_sin: float = Field(
        description="Sine of the wind direction at 9am", ge=-1, le=1
    )
    WindDir9am_cos: float = Field(
        description="Cosine of the wind direction at 9am", ge=-1, le=1
    )
    WindDir3pm_sin: float = Field(
        description="Sine of the wind direction at 3pm", ge=-1, le=1
    )
    WindDir3pm_cos: float = Field(
        description="Cosine of the wind direction at 3pm", ge=-1, le=1
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "MinTemp": 12.3,
                    "MaxTemp": 24.7,
                    "Rainfall": 0.0,
                    "Evaporation": 5.6,
                    "Sunshine": 9.8,
                    "WindGustSpeed": 35.0,
                    "WindSpeed9am": 20.0,
                    "WindSpeed3pm": 24.0,
                    "Humidity9am": 82.0,
                    "Humidity3pm": 55.0,
                    "Pressure9am": 1015.0,
                    "Pressure3pm": 1012.0,
                    "Cloud9am": 4.0,
                    "Cloud3pm": 3.0,
                    "Temp9am": 17.2,
                    "Temp3pm": 22.4,
                    "RainToday": 0,
                    "Month": 8,
                    "Year": 2023,
                    "Day": 24,
                    "Latitude": -33.86,
                    "Longitude": 151.21,
                    "WindGustDir_sin": 0.7071,
                    "WindGustDir_cos": 0.7071,
                    "WindDir9am_sin": 0.5,
                    "WindDir9am_cos": 0.866,
                    "WindDir3pm_sin": -0.5,
                    "WindDir3pm_cos": -0.866
                }
            ]
        }
    }


# Clase de salida para el modelo
class ModelOutput(BaseModel):
    int_output: bool
    str_output: Literal["No rain tomorrow", "Rain expected tomorrow"]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "int_output": True,
                    "str_output": "Rain expected tomorrow"
                }
            ]
        }
    }


# Cargar el modelo antes de iniciar
model, version_model = load_model("weather_model_prod", "champion")

app = FastAPI()


@app.get("/")
async def read_root():
    return JSONResponse(content=jsonable_encoder({"message": "Welcome to the Weather Prediction API"}))


@app.post("/predict/", response_model=ModelOutput)
def predict(
    features: Annotated[ModelInput, Body(embed=True)],
    background_tasks: BackgroundTasks
):
    features_dict = features.dict()
    features_list = [features_dict[key] for key in features_dict.keys()]

    # Convertir a DataFrame
    features_df = pd.DataFrame([features_list], columns=features_dict.keys())

    # Realizar la predicción
    prediction = model.predict(features_df)

    # Convertir el resultado a cadena legible
    str_pred = "No rain tomorrow" if prediction[0] == 0 else "Rain expected tomorrow"

    # Verificar cambios de modelo en segundo plano
    background_tasks.add_task(load_model, "weather_model_prod", "champion")

    # Retornar la predicción
    return ModelOutput(int_output=bool(prediction[0]), str_output=str_pred)
