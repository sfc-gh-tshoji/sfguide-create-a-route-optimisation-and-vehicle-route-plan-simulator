# Create-a-Route-Optimisation-and-Vehicle-Route-Plan-Simulator

This tutorial leverages the [Open Route Service](https://openrouteservice.org/) to optimise vehicle routes in order to distribute goods to chosen destinations on time.

The results are flexible in terms of location - you can choose to simulate routes from anywhere in the world.

The open route service is free to use but there are restrictions in the number of calls to the freely available api api.

If you need a solution  without imits, you may wish  to install the services inside a container.

https://openrouteservice.org/plans/

Either way, Snowflake allows for creation of a fully interactive route simulator which will benefit many vehicle centric industries such as **retail**, **distribution**, **healthcare** and more.

# Setup
### Create Database and Warehouse

```sql

CREATE DATABASE IF NOT EXISTS VEHICLE_ROUTING_SIMULATOR;
CREATE WAREHOUSE IF NOT EXISTS ROUTING_ANALYTICS;
```

Create 2 stages, one for the notebook, and the other for the Streamlit

```sql
 CREATE STAGE IF NOT EXISTS VEHICLE_ROUTING_SIMULATOR.routing.notebook DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

 CREATE STAGE IF NOT EXISTS VEHICLE_ROUTING_SIMULATOR.routing.streamlit DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
```


### Add files to stages

Click [here](Snowflake-Labs/Create-a-Route-Optimisation-and-Vehicle-Route-Plan-Simulator/Notebook) and download the contents of the folder

Upload **all** contents into the notebook stage

Click [here](Snowflake-Labs/Create-a-Route-Optimisation-and-Vehicle-Route-Plan-Simulator/Streamlot) and download the contents of the folder


Upload **all** contents into the streamlit stage - Ensure the config.toml file is uploaded into a folder called .streamlit

### Create Notebook and Streamlit from Stages

```sql

--notebook
CREATE OR REPLACE NOTEBOOK VEHICLE_ROUTING_SIMULATOR.ROUTING.ROUTING_DEMO_SETUP
FROM 'VEHICLE_ROUTING_SIMULATOR.ROUTING.NOTEBOOK'
MAIN_FILE = 'routing_setup.ipynb'
QUERY_WAREHOUSE = 'ROUTING_ANALYTICS';

ALTER NOTEBOOK VEHICLE_ROUTING_SIMULATOR.ROUTING.VEHICLE_ROUTING_DEMO_SETUP ADD LIVE VERSION FROM LAST;

--streamlit
CREATE OR REPLACE STREAMLIT VEHICLE_ROUTING_SIMULATOR.ROUTING.VEHICLE_ROUTING_OPTIMISATION_SIMULATION
ROOT_LOCATION = 'VEHICLE_ROUTING_SIMULATOR.routing.streamlit'
MAIN_FILE = 'routing.py'
QUERY_WAREHOUSE = 'ROUTING_ANALYTICS'
COMMENT = '{"origin":"sf_sit", "name":"Dynamic Route Optimisation Streamlit app", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":0, "source":"streamlit"}}';

```

### Begin the tutorial

Now you have a configure streamlit and notebook, you can start the lab.

- Navigate to the Routing_demo_setup notebook and follow the instructions inside

- when complete navigate to the vehicle routing optimisation simulation app