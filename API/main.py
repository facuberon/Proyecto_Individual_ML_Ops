
from fastapi import FastAPI
import pandas as pd
import numpy as np
import fastparquet


# Cargo los datos del Dataset final con la variable df
df = pd.read_parquet("final.parquet")
#creacion de una aplicacion
# aumenta el tiempo de espera a 60 segundos para todas las rutas
app = FastAPI()

@app.get("/get_max_duration")
async def get_max_duration(year:int= None, platform: str = None, duration_type: str = None):
    #Esta funcion devuelve el título de la película con la duración máxima.
    #Recibe los parametros year, platform y duration_type (siendo las tres opcionales)
    df = pd.read_parquet("final.parquet")
      #filtro por parametros 
    if year:
        df = df[df['release_year'] == year]
    
    if platform:
        df = df[df['plataform'] == platform]
    
    if duration_type:
        if duration_type == 'min':
            max_duration = df.sort_values(by='duration_type', ascending=True).iloc[0]['title']
        elif duration_type == 'season':
            max_duration = df.sort_values(by='duration_type', ascending=True).iloc[0]['title']
        else:
            return {'error': 'Invalid duration type'}

  #Si no se coloca parametro devuelve el titulo con maxima duracion
    else:
        max_duration = df.loc[df['duration_int'].idxmax(), 'title']

  # obtengo el título de la película con duración máxima
        
    return {'title': max_duration}

@app.get("/get_score_count")
async def get_score_count(platform: str, scored: float, year: int):
    #Esta funcion cuenta la cantidad de películas que cumplen con los criterios 
    #ingresados en "platform, scored, year" y muestra el total.
    #Resive los parametros platform, scored, year (que no son opcionales)

    #selecciona las peliculas que cumplan con el criterio de los parametros
    selec = df.loc[(df['plataform'] == platform) & (df['score_y'] >= scored) & (df['release_year'] == year)]

    #contar las peliculas y retornar el resultado
    contar = selec['title']

    #Seteo los duplicados y los cuento con len
    contar= set(contar)

    return len(contar)


@app.get("/get_count_platform")
async def get_count_platform(platform: str):
    #Esta Funcion filtra el Dataset por la plataforma especificada, luego las
    #setea para evitar duplicados y retorna la cantidad de películas.
    #Recibe el parametro platform(no opcional)

    #filtro las películas a la plataforma especificada
    peliculas_filtradas = df[df["plataform"] == platform]
  
    #cuento los titulos de la plataforma filtrada
    contar = peliculas_filtradas['title']

    #Seteo la cantidad de peliculas encontradas y retorno el numero total sin duplicados
    cantidad_peliculas= set(contar)

    return len(cantidad_peliculas)

@app.get("/get_actor")
async def get_actor(platform: str, year: int):
    #Esta Funcion filtra el Dataset por la plataforma especificada y 
    # el año de lanzamiento, luego retorna el nombre del actor mas repetido.
    #Recibe los parametros platform y year (no son opcionales)

    # Filtro por plataforma y año
    filtro = df[(df['plataform'] == platform) & (df['release_year'] == year)]

    #Reemplazo los valores nulos en la columna "cast" por "ningun actor"
    filtro['cast'].fillna(value='ningun actor', inplace=True)

    # Creo una lista de actores
    actores = filtro['cast'].str.split(', ')

    # Creo un Dataset a partir de la lista de actores
    actores = pd.DataFrame({'actor': [actor for actors in actores for actor in actors]})

    # Filtro por actores distintos
    actores_filtro = actores[actores['actor'] != 'ningun actor'].groupby('actor').size().reset_index(name='count')

    # Ordeno de mayor a menor y obtengo el nombre del actor con más apariciones
    actor = actores_filtro.sort_values('count', ascending=False)['actor'].iloc[0]

    return actor
