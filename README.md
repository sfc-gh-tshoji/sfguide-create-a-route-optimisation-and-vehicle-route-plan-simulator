# sfguide-create-a-route-optimisation-and-vehicle-route-plan-simulator

## Overview

This tutorial leverages the [Open Route Service](https://openrouteservice.org/) to optimise vehicle routes in order to distribute goods to chosen destinations on time.

The results are flexible in terms of location - you can choose to simulate routes from anywhere in the world.

The open route service is free to use but there are restrictions in the number of calls to the freely available api api.

If you need a solution  without imits, you may wish  to install the services inside a container.

https://openrouteservice.org/plans/

Either way, Snowflake allows for creation of a fully interactive route simulator which will benefit many vehicle centric industries such as **retail**, **distribution**, **healthcare** and more.

## Setup

Click [Here](https://quickstarts.snowflake.com/guide/Create-a-Route-Optimisation-and-Vehicle-Route-Plan-Simulator) for a guide on how to setup the solution along with detailed narrative on how the Streamlit code works.


#### Considerations
The Job details may plot routes outside the agreed time.  The Demo has only vehicles where each vehicle has a unique skill.  We will need more vehicles / less skills to prevent these violations.



