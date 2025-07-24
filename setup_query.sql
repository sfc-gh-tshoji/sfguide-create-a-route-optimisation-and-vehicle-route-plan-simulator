/***
 Native app & SPCS セットアップ
***/
use schema openrouteservice_setup.public;

-- ファイルアップロード後、ステージ更新
ALTER STAGE OPENROUTESERVICE_SETUP.PUBLIC.ORS_SPCS_STAGE REFRESH;

-- ディレクトリテーブル確認
select * from directory(@OPENROUTESERVICE_SETUP.PUBLIC.ORS_SPCS_STAGE);

-- spcs_setup.sh 実行後のImage repository確認
SHOW IMAGES IN IMAGE REPOSITORY IMAGE_REPOSITORY;

-- サービス起動
alter service OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SERVICE RESUME;
alter service OPENROUTESERVICE_NATIVE_APP.CORE.ROUTING_GATEWAY_SERVICE RESUME;
alter service OPENROUTESERVICE_NATIVE_APP.CORE.DOWNLOADER RESUME;
alter service OPENROUTESERVICE_NATIVE_APP.CORE.VROOM_SERVICE RESUME;

/***
 ORS API コール
***/
-- データベース、スキーマ、ウェアハウス作成
CREATE DATABASE IF NOT EXISTS VEHICLE_ROUTING_SIMULATOR;
CREATE WAREHOUSE IF NOT EXISTS ROUTING_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS DATA;

-- ORS API トークン用のシークレット作成
CREATE SECRET IF NOT EXISTS CORE.ROUTING_TOKEN
  TYPE = GENERIC_STRING
  SECRET_STRING = ''
  COMMENT = 'token for routing demo'

-- ネットワークルール、外部アクセス統合作成
CREATE OR REPLACE NETWORK RULE open_route_api
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.openrouteservice.org');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION open_route_integration
  ALLOWED_NETWORK_RULES = (open_route_api)
  ALLOWED_AUTHENTICATION_SECRETS = all
  ENABLED = true;

-- UDF作成
-- ディレクション取得（緯度経度を用いたルート案内）
CREATE OR REPLACE FUNCTION CORE.DIRECTIONS (method varchar, jstart array, jend array)
RETURNS VARIANT
language python
runtime_version = 3.10
handler = 'get_directions'
external_access_integrations = (OPEN_ROUTE_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = CORE.ROUTING_TOKEN )

AS
$$
import requests
import _snowflake
def get_directions(method,jstart,jend):
    request = f'''https://api.openrouteservice.org/v2/directions/{method}'''
    key = _snowflake.get_generic_secret_string('cred')

    PARAMS = {'api_key':key,
            'start':f'{jstart[0]},{jstart[1]}', 'end':f'{jend[0]},{jend[1]}'}

    r = requests.get(url = request, params = PARAMS)
    response = r.json()
    
    return response
$$;

-- ディレクション取得（ロケーション情報を用いたルート案内）
CREATE OR REPLACE FUNCTION CORE.DIRECTIONS (method varchar, locations variant)
RETURNS VARIANT
language python
runtime_version = 3.9
handler = 'get_directions'
external_access_integrations = (OPEN_ROUTE_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = CORE.ROUTING_TOKEN )

AS
$$
import requests
import _snowflake
import json

def get_directions(method,locations):
    request_directions = f'''https://api.openrouteservice.org/v2/directions/{method}/geojson'''
    key = _snowflake.get_generic_secret_string('cred')

    HEADERS = { 'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
               'Authorization':key,
               'Content-Type': 'application/json; charset=utf-8'}

    body = locations

    r = requests.post(url = request_directions,json = body, headers=HEADERS)
    response = r.json()
    
    return response

    $$;

-- 最適化関数
CREATE OR REPLACE FUNCTION CORE.OPTIMIZATION (jobs array, vehicles array)
RETURNS VARIANT
language python
runtime_version = 3.9
handler = 'get_optimization'
external_access_integrations = (OPEN_ROUTE_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = CORE.ROUTING_TOKEN )

AS
$$
import requests
import _snowflake
def get_optimization(jobs,vehicles):
    request_optimization = f'''https://api.openrouteservice.org/optimization'''
    key = _snowflake.get_generic_secret_string('cred')
    HEADERS = { 'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
               'Authorization':key,
               'Content-Type': 'application/json; charset=utf-8'}

    body = {"jobs":jobs,"vehicles":vehicles}

    r = requests.post(url = request_optimization,json = body, headers=HEADERS)
    response = r.json()
    
    return response
$$;

-- 等時間等距離図作成
CREATE OR REPLACE FUNCTION CORE.ISOCHRONES(method string, lon float, lat float, range int)
RETURNS VARIANT
language python
runtime_version = 3.9
handler = 'get_isochrone'
external_access_integrations = (OPEN_ROUTE_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = CORE.ROUTING_TOKEN )

AS
$$
import requests
import _snowflake
def get_isochrone(method,lon,lat,range):
    request_isochrone = f'''https://api.openrouteservice.org/v2/isochrones/{method}'''
    key = _snowflake.get_generic_secret_string('cred')
    HEADERS = { 'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
               'Authorization':key,
               'Content-Type': 'application/json; charset=utf-8'}

    body = {'locations':[[lon,lat]],
                    'range':[range*60],
                    'location_type':'start',
                    'range_type':'time',
                    'smoothing':10}

    r = requests.post(url = request_isochrone,json = body, headers=HEADERS)
    response = r.json()
    
    return response
$$;

/***
 CARTO データセット取得（Snowflakeマーケットプレイス）
***/

-- GUIで実施
-- データセット名: Overture Maps - Places

/***
 AISQLによるルーティング
***/

-- クロスリージョンの有効化
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- データベース、スキーマ、ウェアハウス作成
CREATE DATABASE IF NOT EXISTS VEHICLE_ROUTING_SIMULATOR;
CREATE WAREHOUSE IF NOT EXISTS ROUTING_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS DATA;
CREATE SCHEMA IF NOT EXISTS NOTEBOOKS;
CREATE SCHEMA IF NOT EXISTS STREAMLITS;

-- ノートブック用ステージ作成
CREATE STAGE IF NOT EXISTS VEHICLE_ROUTING_SIMULATOR.NOTEBOOKS.notebook 
DIRECTORY = (ENABLE = TRUE) 
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
--> ipynbファイル, environment.ymlをアップロード

-- ステージ内のipynbファイルからノートブック作成
CREATE OR REPLACE NOTEBOOK VEHICLE_ROUTING_SIMULATOR.NOTEBOOKS.EXPLORE_ROUTING_FUNCTIONS_WITH_AISQL
FROM '@VEHICLE_ROUTING_SIMULATOR.NOTEBOOKS.NOTEBOOK'
MAIN_FILE = 'routing_setup.ipynb'
QUERY_WAREHOUSE = 'ROUTING_ANALYTICS'
COMMENT = '{"origin":"sf_sit-is", "name":"Route Optimization with Open Route Service", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"notebook"}}';

ALTER NOTEBOOK VEHICLE_ROUTING_SIMULATOR.NOTEBOOKS.EXPLORE_ROUTING_FUNCTIONS_WITH_AISQL ADD LIVE VERSION FROM LAST;

/***
 Streamlit デプロイ
***/

-- Streamlit用ステージ作成
CREATE STAGE IF NOT EXISTS VEHICLE_ROUTING_SIMULATOR.STREAMLITS.STREAMLIT 
DIRECTORY = (ENABLE = TRUE) 
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Streamlitアプリのデプロイ
ls @VEHICLE_ROUTING_SIMULATOR.STREAMLITS.streamlit;

CREATE OR REPLACE STREAMLIT VEHICLE_ROUTING_SIMULATOR.STREAMLITS.ROUTE_OPTIMIZATION_SIMULATOR
FROM '@VEHICLE_ROUTING_SIMULATOR.STREAMLITS.streamlit'
MAIN_FILE = 'routing.py'
QUERY_WAREHOUSE = 'ROUTING_ANALYTICS'
COMMENT = '{"origin":"sf_sit-is", "name":"Route Optimization with Open Route Service", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"Streamlit"}}';