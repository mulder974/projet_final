import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from datetime import datetime
import geopandas
from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, DateTime, ForeignKey, Numeric, Float, Date, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

load_dotenv()
username = os.getenv("user")
user_password = os.getenv("password")

engine = create_engine(
    f"postgresql+psycopg2://{username}:{user_password}@localhost/energy", max_overflow=20, pool_size=0)

Base = declarative_base()
Session = sessionmaker(bind=engine)
engine.connect()

class Region(Base):
  __tablename__ = 'regions'
  id = Column(Integer, primary_key=True)
  name = Column(String)

class Location(Base):
  __tablename__ = 'locations'
  id = Column(Integer, primary_key=True)
  name = Column(String)
  geo_localisation = Column(Geometry('MULTIPOLYGON'))
  region_id = Column(Integer, ForeignKey('regions.id'))
  region = relationship('Region', backref='locations')

class Energy_type(Base):
  __tablename__ = 'energy_types'
  id = Column(Integer, primary_key=True)
  name = Column(String)

class Energy_bilan(Base):
  __tablename__ = 'energy_bilans'
  id = Column(Integer, primary_key=True)
  location_id = Column(Integer, ForeignKey('locations.id'))
  location = relationship('Location', backref='energy_bilans')
  energy_type_id = Column(Integer, ForeignKey('energy_types.id'))
  energy_type = relationship('Energy_type', backref='energy_bilans')
  year = Column(Date)
  production = Column(Float)
  consumption = Column(Float)

def create_tables():
  '''Crée toutes les tables'''
  Base.metadata.create_all(engine)

def drop_tables():
  '''Supprime toutes le tables et leur contenu'''
  Base.metadata.drop_all(engine)

def add_region():
  '''Remplis la table 'regions' depuis le csv'''
  df = pd.read_csv('Sauvegardes_clean_csv/regions.csv', index_col=0)
  df.to_sql('regions', engine, if_exists='append', index=False)

def add_energy_type():
  '''Remplis la table 'energy_types' depuis le csv'''
  df = pd.read_csv('Sauvegardes_clean_csv/energy_types.csv', index_col=0)
  df.to_sql('energy_types', engine, if_exists='append', index=False)

def add_location():
  '''Remplis la table 'locations' depuis le csv'''
  df = pd.read_csv('Sauvegardes_clean_csv/locations.csv', index_col=0)
  df.to_sql('locations', engine, if_exists='append', index=False)

def add_bilan():
  types_list = ['Primary', 'Oil', 'Gas', 'Coal', 'Nuclear',
                'Hydro', 'Solar', 'Wind', 'Geo Biomass', 'Biofuel']
  for type_ in types_list:
    df = pd.read_csv(f'Sauvegardes_clean_csv/cons_csv/{type_}.csv', index_col=0)
    session = Session()
    query = session.query(Energy_type).filter(Energy_type.name == type_)
    res_list = query.all()
    energy_type_id = res_list[0].id
    for country in df:
      query2 = session.query(Location).filter(Location.name == country)
      res_list2 = query2.all()
      location_id = res_list2[0].id
      for index, row in df.iterrows():
        index = datetime.strptime(str(index), '%Y')
        energy_bilan = Energy_bilan( location_id = location_id,
              energy_type_id = energy_type_id,
              year = index,
              consumption = row[country])
        session.add(energy_bilan)
        session.commit()

def add_prod():
  types_list = ['Oil', 'Gas', 'Coal', 'Nuclear',
                'Hydro', 'Solar', 'Wind', 'Geo Biomass', 'Biofuel']
  for type_ in types_list:
    df = pd.read_csv(f'Sauvegardes_clean_csv/prod_csv/{type_}.csv', index_col=0)
    session = Session()
    query = session.query(Energy_type).filter(Energy_type.name == type_)
    res_list = query.all()
    energy_type_id = res_list[0].id
    for country in df:
      query2 = session.query(Location).filter(Location.name == country)
      res_list2 = query2.all()
      location_id = res_list2[0].id
      for index, row in df.iterrows():
        index = datetime.strptime(str(index), '%Y')
        query3= session.query(Energy_bilan).filter(and_(Energy_bilan.energy_type_id == energy_type_id,
          Energy_bilan.location_id == location_id,
          Energy_bilan.year == index))
        result = query3.all()
        if result:
            x = session.query(Energy_bilan).get(result[0].id)
            x.production = row[country]
            session.commit()
        else :
          energy_bilan = Energy_bilan(location_id=location_id,
                                      energy_type_id=energy_type_id,
                                      year=index,
                                      production=row[country])
          session.add(energy_bilan)
          session.commit()


# drop_tables()
# create_tables()
# add_region()
# add_energy_type()
# add_location()
# add_bilan()
# add_prod()


