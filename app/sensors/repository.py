import json
from ..mongodb_client import MongoDBClient
from app.redis_client import RedisClient
from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return db_sensor
def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    db_sensor_data = sensor.dict()
    mongodb.insert(db_sensor_data)  # Insert the sensor data in the mongodb_collection('sensors')

    return db_sensor

def record_data(redis: RedisClient, db_sensor: models.Sensor, data: schemas.SensorData):
    # Store the sensor data in Redis
    json_data = json.dumps(dict(data))  # Serialize the dictionary to JSON (convert SensorData to JSON)
    redis.set(db_sensor.id, json_data)
    # Return the recorded data 
    return data

def get_data(redis: RedisClient, db_sensor: models.Sensor):
   # Fetch the static sensor info from the database
    db_sensor_info = vars(db_sensor) # Convert db_sensor into dict().
    # Fetch the sensor data from Redis
    redis_sensor_data = redis.get(db_sensor.id)
    if redis_sensor_data is None:
        raise HTTPException(status_code=404, detail="Sensor data not found")
    
    # Deserialize Redis sensor data
    redis_sensor_dict = json.loads(redis_sensor_data)

    # Combine the static and variable sensorCreate and SensorData and return both as dict().
    combined_data = {
        **db_sensor_info,  # Static sensor info from sql_db
        **redis_sensor_dict  # Variable sensor data from Redis
    }
    return combined_data

# We delete the sensor from PostgreSQL, Redis and MongoDB
def delete_sensor(db: Session, sensor_id: int, mongodb: MongoDBClient, redis: RedisClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    mongodb.delete(db_sensor.name)
    redis.delete(sensor_id)
    return db_sensor

# We use the mongdb querys to do this method
def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float, radius: float, redis: RedisClient, db: Session):
    near = []
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
     "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}

    sensors = mongodb.collection.find(query) # Do a query for the sensors in a given radius.
    for sensor in sensors:  # Traverse for every sensor in the doc.
        db_sensor = get_sensor_by_name(db,sensor['name'])
        db_sensor_data = get_data(redis, db_sensor) 

        near.append(db_sensor_data)
    return near