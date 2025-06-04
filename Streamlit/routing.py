# Import python packages
import altair as alt

import streamlit as st col('PROFILE'),object_construct(lit('coordinates'),col('LINE'))).alias('DIRECTIONS')).cache_result()
from snowflake.snowpark.context import get_active_session
import requests
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import pandas as pd
import pydeck as pdk
import json
from snowflake.snowpark.window import Window
session = get_active_session()
st.set_page_config(layout="wide")

#color scheme

st.markdown(
    """
    <style>
    .heading{
        background-color: rgb(41, 181, 232);  /* light blue background */
        color: white;  /* white text */
        padding: 60px;  /* add padding around the content */
    }
    .tabheading{
        background-color: rgb(41, 181, 232);  /* light blue background */
        color: white;  /* white text */
        padding: 10px;  /* add padding around the content */
    }
    .veh1 {
        color: rgb(125, 68, 207);  /* purple */
    }
    .veh2 {
        color: rgb(212, 91, 144);  /* pink */
    }
    .veh3 {
        color: rgb(255, 159, 54);  /* orange */
    }
    .veh4 {
        padding: 10px;  /* add padding around the content */
        color: rgb(0,53,69);  /* midnight */
    }
    
    body {
        color: rgb(0,53,69);
    }
    
    div[role="tablist"] > div[aria-selected="true"] {
        background-color: rgb(41, 181, 232);
        color: rgb(0,53,69);  /* Change the text color if needed */
    }
    
    </style>
    """,
    unsafe_allow_html=True
)


# Logo for Sidebar

image = 'https://www.snowflake.com/wp-content/themes/snowflake/assets/img/brand-guidelines/logo-white-example.svg'

# industry lookup
lookup = session.table('LOOKUP')
industry = lookup.select('INDUSTRY')

# Align industry based on user selection

with st.sidebar:
    st.image(image)
    choice = st.radio('select industry', industry)
    lookup = lookup.filter(col('INDUSTRY')==choice)
    lookup = lookup.with_column('IND',array_to_string('IND',lit(',')))
    lookup = lookup.with_column('IND2',array_to_string('IND2',lit(',')))
    
    lookuppd = lookup.to_pandas()

# filters industry, sets relevant product types, and asigns customer type liet
    
    pa = lookuppd.PA.iloc[0]
    pb = lookuppd.PB.iloc[0]
    pc = lookuppd.PC.iloc[0]
    ind = lookuppd.IND.iloc[0]
    ind2 = lookuppd.IND2.iloc[0]
    ctype = json.loads(lookuppd.CTYPE.iloc[0])
    stype = json.loads(lookuppd.STYPE.iloc[0])


        
    

st.markdown('<h1 class="heading">ROUTE OPTIMISATION SIMULATION</h2>', unsafe_allow_html=True)

st.markdown('<h6 class="veh4">Powered by the Open Route Service https://openrouteservice.org/</h2>', unsafe_allow_html=True)

####### standing data


###### drop down for method for each vehicle - 
######in this case we are using 3 vehicles with method of choice

method =['driving-car',
             'driving-hgv',
             'cycling-regular',
             'cycling-road',
             'cycling-mountain',
            'cycling-electric']



####### point of interest data from carto - optimised in the initial setup

places_f = session.table('places')

places_f = places_f.select('GEOMETRY',call_function('ST_X',col('GEOMETRY')).alias('LON'),
                        call_function('ST_Y',col('GEOMETRY')).alias('LAT'),
                           col('ADDRESS'),col('CATEGORY'),col('ALTERNATE'),col('PHONES'),col('NAME'),col('GEOMETRY').alias('POINT'))


#########cortex map filter

prompt = ('give me the LAT and LON which centers the following place')


with st.sidebar:
    st.markdown('##### Cortex Powered Map Filter')
    st.info(prompt)
    model = st.selectbox('Choose Model:',['reka-flash','mistral-large2'],1)
    place = st.text_input('Choose Input','Heathrow Airport')
    distance = st.number_input('Distance from location in KM',1,300,5)

# function for cotex powered location search.  After search the coordinates are cached in session to minimise calls.

@st.cache_data
def choose_place(place,model,distance):

    json_template = str({'LAT':44.3452,'LON':2.345,'DISTANCE':100})
    min_max = session.createDataFrame([{'PLACE':place}])\
    .with_column('CENTER',call_function('snowflake.cortex.complete',model,
                                  concat(lit(prompt),
                                  col('PLACE'),lit(f'return 3 values which are as follows LAT and LON with {distance} as DISTANCE'),
                                                   lit('use the following json template'),
                                                   lit(json_template),lit('return only json. DO NOT RETURN COMMENTRY OR VERBIAGE'))
                                  ))
    
# formatting resullts
    min_max = min_max.select(parse_json(replace(replace('CENTER',"```",''),'json','')).alias('CENTER'))
    return min_max.select(col('CENTER')['LAT'].astype(FloatType()).alias('LAT'),
                          col('CENTER')['LON'].astype(FloatType()).alias('LON'),
                          call_function('ST_ASWKT',call_function('ST_MAKEPOINT',col('LON'),col('LAT'))).alias('POINT'),
                          col('CENTER')['DISTANCE'].astype(FloatType()).alias('"DISTANCE1"'),
                          lit(0).alias('DISTANCE'),
                         lit(place).alias('NAME')).to_pandas()
    



##########




####### BOUNDING BOX FILTER - addition to the sidebar which produces cortex results and produces the point in question on a map

with st.sidebar:
    
    bbox = choose_place(place,model,distance)
    st.caption(f'Latitude:{bbox.LAT.iloc[0]}')
    st.caption(f'Longitude:{bbox.LON.iloc[0]}')

    context = pdk.Layer(
    'ScatterplotLayer',
    bbox,
    get_position=['LON', 'LAT'],
    filled=True,
    stroked=False,
    radius_min_pixels=6,
    radius_max_pixels=20,
    auto_highlight=True,
    get_fill_color=[41, 181, 232],
    pickable=True)
    st.divider()

    

    

#### filter the places for wholeseller based on cortex input


places_w = places_f.filter(call_function('ST_DWITHIN', places_f['GEOMETRY'],to_geography(lit(bbox.POINT.iloc[0])),lit(bbox.DISTANCE1.iloc[0])*1000))#.cache_result()

### search accross categories and alternate fields ---- here its searching accroos two sets of key words
places_1 = places_w.filter(expr(f'''search((CATEGORY,ALTERNATE,NAME),'{ind}',analyzer=>'DEFAULT_ANALYZER')''')).cache_result()
places_1 = places_1.filter(expr(f'''search((CATEGORY,ALTERNATE,NAME),'{ind2}',analyzer=>'DEFAULT_ANALYZER')''')).cache_result()

# customise the job template based on industry selection

time_slots = session.table('JOB_TEMPLATE')
pa = time_slots.filter(col('PRODUCT')=='pa').join(lookup.select('PA'))
pb = time_slots.filter(col('PRODUCT')=='pb').join(lookup.select('PB'))
pc = time_slots.filter(col('PRODUCT')=='pc').join(lookup.select('PC'))

time_slots = pa.union(pb).union(pc).with_column('PRODUCT',col('PA')).drop('PA')


# catch all - exception if no places within selected point are found

if places_1.to_pandas().empty:
    st.info(f'No {ind} found')

# retrieve all potential distributors within specified point

else:

    places_1 = places_1.with_column('DISTANCE',call_function('ST_DISTANCE',
                                                         call_function('ST_MAKEPOINT',col('LON'),col('LAT')),
                                                        call_function('ST_MAKEPOINT',lit(bbox.LON.iloc[0]),lit(bbox.LAT.iloc[0]))))


    places_1 = places_1.with_column('DISTANCE',round(col('DISTANCE')/1000,2)).order_by('DISTANCE')

    @st.cache_data
    def places_cached(distance,bbox,ind):
        return places_1.to_pandas()

    # Generate tooltip and second layer in pydeck which shows all suggested distributors

    tooltip = {
   "html": """<b>Name:</b> {NAME} <b><br>Distance From Centre:</b> {DISTANCE}""",
   "style": {
       "width":"50%",
        "backgroundColor": "steelblue",
        "color": "white",
       "text-wrap": "balance"
            }
        }



    wholesalers = pdk.Layer(
    'ScatterplotLayer',
    places_cached(distance,bbox,ind),
    get_position=['LON', 'LAT'],
    filled=True,
    opacity=0.5,
    stroked=False,
    radius_min_pixels=6,
    radius_max_pixels=10,
    auto_highlight=True,
    get_fill_color=[0, 0, 0],
    pickable=True)
    st.divider()


    view_state = pdk.ViewState(bbox.LON.iloc[0], bbox.LAT.iloc[0], zoom=4)

    ## add map to the side baralong with another selectbox which allows users to choose from list of distributors

    with st.sidebar:
        @st.cache_data
        def warehouses(distance,bbox,ind):
            return places_1.group_by(col('NAME')).agg(avg('DISTANCE').alias('DISTANCE')).sort(col('DISTANCE').asc()).to_pandas()

        st.caption(f'Suppliers within a {distance}km square area around {place}')



    

        st.pydeck_chart(pdk.Deck(layers=[wholesalers,context],map_style=None,initial_view_state=view_state, tooltip=tooltip))
    
    
        s_warehouse = st.selectbox('Choose Wholesaler to distribute goods from:',warehouses(distance,bbox,ind))
    
        customer_type = st.multiselect('Choose The Type of Customer:', ctype,ctype[0])
        places_1 = places_1.filter(col('NAME')==s_warehouse).limit(1)

    job_startlon = places_1.select('LON','LAT').collect()[0][0]
    job_startlat = places_1.select('LON','LAT').collect()[0][1]


    address = places_1.select(col('POINT'),
                          col('NAME'),
                          col('ADDRESS')['country'].astype(StringType()).alias('COUNTRY'),
                         col('ADDRESS')['freeform'].astype(StringType()).alias('ADDRESS_1'),
                         col('ADDRESS')['locality'].astype(StringType()).alias('LOCALITY'),
                         col('ADDRESS')['postcode'].astype(StringType()).alias('ZIP'),
                         col('PHONES').astype(StringType()).alias('PHONE_NUMBER')).to_pandas()

    st.markdown(f'#### Wholesaler: {address.NAME.iloc[0]}')
    st.markdown(f'**Address:** {address.ADDRESS_1.iloc[0]}, {address.LOCALITY.iloc[0]}, {address.ZIP.iloc[0]}')
    st.markdown(f'**Telephone:** {address.PHONE_NUMBER.iloc[0]}')

    # vehicle setup - each vehicle has a skill and a capacity - these get matched with jobs - if no match on job is found then job will be unassigned.
    # Vehicles will be transferred to setup in notebook to add more flexibility

    st.divider()

    st.markdown('#### Vehicles that can deliver goods')

    skill_types = (stype)

    veh1_skills = [1]
    veh2_skills = [2]
    veh3_skills = [3]

    veh1_capacity = [4]
    veh2_capacity = [6]
    veh3_capacity = [12]


    st.markdown('<h4 class="veh1">Vehicle 1</h2>', unsafe_allow_html=True, help=f'''Vehicle 1 {skill_types[veh1_skills[0]-1]} and a capacity of {veh1_capacity[0]}''')
    col1,col2,col3 = st.columns(3)
    with col1:
        start_time_0 = st.number_input('Start Time in Hours:',0,24,8,key=1)
    with col2:
        end_time_0 = st.number_input('End Time in Hours:',start_time_0,24,17,key=2)
    with col3:
        smethod = st.selectbox('Choose Method:',method, key=5)
    
                    
    
    
    
    
    places_vehicles = places_1.filter(col('NAME')==s_warehouse).cache_result()
    
    
    vehicle_1 = places_vehicles.select(object_construct(lit('profile'),
                                                                             lit(smethod),
                                                                              lit('skills'),
                                                                              lit(veh1_skills),
                                                                             lit('id'),
                                                                              lit(1),
                                                                            lit('start'),
                                                                             array_construct(col('LON'),col('LAT')),
                                                                            lit('end'),
                                                                              array_construct(col('LON'),col('LAT')),
                                                                             lit('time_windows'),
                                                                            array_construct(lit(start_time_0*60*60),lit(end_time_0*60*60)),
                                                                            lit('capacity'),
                                                                             lit(veh1_capacity)).alias('VEHICLE'))



    st.markdown('<h4 class="veh2">Vehicle 2</h2>', unsafe_allow_html=True, help=f'''Vehicle 2 {skill_types[veh2_skills[0]-1]} and a capacity of {veh2_capacity[0]}''')
    col4,col5,col6 = st.columns(3)
    with col4:
        start_time_1 = st.number_input('Start Time in Hours:',0,24,8,key=3)
    with col5:
        end_time_1 = st.number_input('End Time in Hours:',start_time_1,24,17,key=4)
    with col6:
        smethod_1 = st.selectbox('Choose Method:',method, key=6)




    vehicle_2 = places_vehicles.select(object_construct(lit('profile'),
                                                                             lit(smethod_1),
                                                                            lit('skills'),
                                                                            lit(veh2_skills),
                                                                             lit('id'),
                                                                              lit(2),
                                                                            lit('start'),
                                                                             array_construct(col('LON'),col('LAT')),
                                                                            lit('end'),
                                                                             array_construct(col('LON'),col('LAT')),
                                                                             lit('time_windows'),
                                                                            array_construct(lit(start_time_1*60*60),lit(end_time_1*60*60)),
                                                                            lit('capacity'),
                                                                           lit(veh2_capacity)).alias('VEHICLE'))



    st.markdown('<h4 class="veh3">Vehicle 3</h2>', unsafe_allow_html=True, help=f'''Vehicle 3 {skill_types[veh3_skills[0]-1]} and a capacity of {veh3_capacity[0]}''')
    col4,col5,col6 = st.columns(3)
    with col4:
        start_time_2 = st.number_input('Start Time in Hours:',0,24,8,key=8)
    with col5:
        end_time_2 = st.number_input('End Time in Hours:',start_time_1,24,17,key=9)
    with col6:
        smethod_2 = st.selectbox('Choose Method:',method, key=10)




    vehicle_3 = places_vehicles.select(object_construct(lit('profile'),
                                                                             lit(smethod_2),
                                                                            lit('skills'),
                                                                            lit(veh3_skills),
                                                                             lit('id'),
                                                                              lit(3),
                                                                            lit('start'),
                                                                             array_construct(col('LON'),col('LAT')),
                                                                            lit('end'),
                                                                             array_construct(col('LON'),col('LAT')),
                                                                             lit('time_windows'),
                                                                            array_construct(lit(start_time_2*60*60),lit(end_time_2*60*60)),
                                                                            lit('capacity'),
                                                                           lit(veh3_capacity)).alias('VEHICLE'))

    


    ##### ADD VEHICLE_COLOR TO VEHICLES)
    vehicle_1 = vehicle_1.with_column('R',lit(125))
    vehicle_1 = vehicle_1.with_column('G',lit(68))  
    vehicle_1 = vehicle_1.with_column('B',lit(207)) 

    vehicle_2 = vehicle_2.with_column('R',lit(212))
    vehicle_2 = vehicle_2.with_column('G',lit(91))  
    vehicle_2 = vehicle_2.with_column('B',lit(144)) 

    vehicle_3 = vehicle_3.with_column('R',lit(255))
    vehicle_3 = vehicle_3.with_column('G',lit(159))  
    vehicle_3 = vehicle_3.with_column('B',lit(54))

    # create vehicle dataframe based on above vehicle setup

    vehsdet = vehicle_1.union(vehicle_2).union(vehicle_3).with_column('ID',col('VEHICLE')['id'])\
    .with_column('PROFILE',col('VEHICLE')['profile'].astype(StringType()))\
    .with_column('WINDOW',col('VEHICLE')['time_windows'].astype(StringType()))
    vehs = vehsdet.select(array_agg('VEHICLE').alias('VEH'))
    vehsdet = vehsdet.drop('VEHICLE')



    start_1 = places_1.select('LON','LAT').collect()[0]


    places_2 = places_f.filter(expr(f'''search((CATEGORY,ALTERNATE,NAME),'{" ".join(customer_type)}',analyzer=>'DEFAULT_ANALYZER')'''))

    with st.sidebar:
        range_minutes = st.number_input('Order Acceptance catchment time:',0,120,20)






submit = st.button('GET JOB ROUTES')

if submit:
    
    #with st.container():
    try:

        isochrone = session.create_dataframe([{'LON':start_1[0], 'LAT':start_1[1], 'METHOD':smethod,'RANGE_MINS':range_minutes}])
        st.write(isochrone)
        
        isochrone = isochrone.select(call_function('UTILS.ISOCHRONES',
                          (col('METHOD'), col('LON'), col('LAT'), col('RANGE_MINS'))).alias('ISOCHRONE'))
    
        #st.write(isochrone.limit(1))
    
        isochrone2 = isochrone.select(to_geography(col('ISOCHRONE')['features'][0]['geometry']).alias('GEO')).cache_result()


    
        places_2 = places_2.join(isochrone2,call_function('ST_WITHIN',places_2['POINT'],isochrone2['GEO'])).sample(0.5).limit(time_slots.count()).cache_result()

        

        window_spec = Window.order_by(random())
        places_2 = places_2.with_column('ID',row_number().over(window_spec))


        places_2 = places_2.join(time_slots,'ID')

        places_2_table = places_2.select('ID',
                                     col('PRODUCT').alias('"Product"'),
                                     col('SLOT_START').alias('"Slot Start"'),
                                     col('SLOT_END').alias('"Slot End"'),
                                     col('NAME').alias('Name'),
                                     col('CATEGORY').alias('"Category"'),
                                     col('ADDRESS')['freeform'].astype(StringType()).alias('"Address"'),
                                     col('ADDRESS')['locality'].astype(StringType()).alias('"Locality"'),
                                     col('ADDRESS')['postcode'].astype(StringType()).alias('"Postcode"'),
                                     col('ADDRESS')['region'].astype(StringType()).alias('"Region"'),
                                     col('PHONES').alias('"Phone Number"'))
                                    



    
        places_2 = places_2.with_column('JOB',object_construct(lit('id'),col('ID'),
                                                                    lit('capacity'),lit([2]),
                                                                    lit('skills'),array_construct(col('SKILLS')),
                                                                    lit('time_window'),
                                                                    array_construct(col('SLOT_START')*60*60,col('SLOT_END')*60*60),
                                                                    lit('location'),array_construct(col('LON'),col('LAT'))
                                                                    ))

    


        jobs = places_2.select(array_agg('JOB').alias('JOB'))
   


        ######## use function to optimise
    
        optim = jobs.join(vehs).select('JOB','VEH',call_function('UTILS.OPTIMIZATION',
                                                             col('JOB'),col('VEH')).alias('OPTIMIZATION'))


    
    
        optim = optim.with_column('CODES',col('OPTIMIZATION')['codes'])
        optim = optim.with_column('ROUTES',col('OPTIMIZATION')['routes'])
        optim = optim.with_column('SUMMARY',col('OPTIMIZATION')['summary'])
        optim = optim.with_column('UNASSIGNED',col('OPTIMIZATION')['unassigned'])
    
    
    
        optim = optim.with_column('COST',col('SUMMARY')['cost'])\
        .with_column('DURATION',col('SUMMARY')['duration'])\
        .with_column('NUMBER_OF_ROUTES',col('SUMMARY')['routes']).drop('SUMMARY')

        optim = optim.join_table_function('flatten',col('ROUTES'))\
        .select('VALUE')
    

        optim = optim.select(col('VALUE')['amount'].alias('AMOUNT'),
                         col('VALUE')['vehicle'].alias('VEHICLE'),
                         col('VALUE')['duration'].alias('DURATION'),
                         col('VALUE')['steps'].alias('STEPS'),
                        col('VALUE')['location'][0].alias('LON'),
                         col('VALUE')['location'][0].alias('LAT')
                        )
   
    
        optim_steps = optim.join_table_function('flatten',col('STEPS'))

        total_time_vehicle = optim_steps.select('VEHICLE',col('VALUE')['duration'].astype(IntegerType())\
                                            .alias('DURATION')).filter(col('VALUE')['type']=='end')

        total_time_vehicle = total_time_vehicle.with_column('DURATION',round(div0(col('DURATION'),lit(60)),2))

    
    
   
                                             
        optim_steps = optim_steps.select('VEHICLE',
                                     col('VALUE')['duration'].astype(IntegerType()).alias('DURATION'),
                                    col('VALUE')['location'][0].astype(FloatType()).alias('LON'),
                                    col('VALUE')['location'][1].astype(FloatType()).alias('LAT'),
                                    col('VALUE')['location'].alias('LOCATION'),
                                    col('VALUE')['type'].astype(StringType()).alias('TYPE'),
                                    col('VALUE')['job'].alias('JOB'))

        


        optim_step_o = optim_steps.sort(col('VEHICLE'),col('DURATION').asc())




        

        optim_line = optim_step_o.group_by('VEHICLE')\
        .agg(array_agg(col('LOCATION')).within_group(col('DURATION').asc()).alias('LINE'),
                                                    count('*').alias('TOTAL_JOBS')).with_column('TOTAL_JOBS',col('TOTAL_JOBS')-2)
        
        
        
        
        
        st.markdown('#### Job Details')
        job_dets = optim_steps.join(places_2_table,optim_steps['JOB']==places_2_table['ID']).drop('LOCATION','ID')
    
        job_dets = job_dets.join(vehsdet,vehsdet['ID']==job_dets['VEHICLE'])
    
    
        job_dets = job_dets.with_column('TIME',parse_json('WINDOW')[0].astype(IntegerType()))
    
        job_dets = job_dets.with_column('DURATION',cast(col('DURATION'),IntegerType()))
    
        job_dets = job_dets.with_column('TIME', time_from_parts(0,0,col('TIME')+col('DURATION')))
    
        job_dets = job_dets.select('VEHICLE',
                               'PROFILE',
                               'TIME',
                                time_from_parts('"Slot End"',0,0).alias('"Agreed Time"'),
                               time_from_parts(0,0,'DURATION').alias('"Cumulative Duration"'),
                               'JOB',
                               '"Product"',
                               'NAME',
                               '"Category"',
                               '"Address"',
                               '"Locality"','"Postcode"','"Region"','"Phone Number"','R','G','B','ID','LAT','LON')

    
        def highlight_first_column(row):
            color = f'background-color: rgb({row["R"]}, {row["G"]}, {row["B"]}); color: white'
            return [color] + [''] * (len(row) - 1)
    
        # Apply the function to the DataFrame
        df = job_dets.drop('LON','LAT').to_pandas()
    
        styled_df = df.style.apply(highlight_first_column, axis=1)

        df_view = df.drop(columns=['R', 'G', 'B'])

        # Display the styled DataFrame in Streamlit
        st.dataframe(styled_df)
    


        optim_line = optim_line.join(vehsdet,vehsdet['ID']==optim_line['VEHICLE'])

    
        optim_line = optim_line\
        .select('VEHICLE','R','G','B','PROFILE','ID','TOTAL_JOBS',
            call_function('UTILS.DIRECTIONS_with_way_points',
                                    col('PROFILE'),col('LINE')).alias('DIRECTIONS')).cache_result()

    
        optim_route_line = optim_line.with_column('GEO',col('DIRECTIONS')['features'][0]['geometry'])

    

        optim_route_line = optim_route_line.with_column('NAME',concat(lit('Vehicle '),col('VEHICLE'),lit(' '),col('PROFILE')))

        


    
        data2 = isochrone2.select('GEO').to_pandas()
        data2["coordinates"] = data2["GEO"].apply(lambda row: json.loads(row)["coordinates"])

        data3 = optim_route_line.select('GEO','PROFILE','NAME','VEHICLE','ID','R','G','B').to_pandas()
        data3["coordinates"] = data3["GEO"].apply(lambda row: json.loads(row)["coordinates"])
        job_detspd = job_dets.with_column('NAME',
                                          concat(lit('<b>'),
                                                 col('"Category"'),
                                                 lit(':</b>'),
                                                 col('NAME'),
                                                 lit('<BR>'),
                                                 lit('<b>Address:</b> '),
                                                 col('"Address"'),
                                                lit('<BR>'),
                                                lit('<b>Postcode:</b> '),
                                                col('"Postcode"'),
                                                lit('<BR>'),
                                                lit('<b>Phone Number:</b> '),
                                                col('"Phone Number"'),
                                                lit('<BR>'),
                                                lit('<b>Cumulate Duration:</b> '),
                                                col('"Cumulative Duration"').astype(StringType()))).drop('TIME','"Agreed Time"','"Cumulative Duration"').to_pandas()
        places_1pd = places_1.to_pandas()
    

        ###### vehicle drops #######
        layer_end = pdk.Layer(
        'ScatterplotLayer',
        job_detspd,
        get_position=['LON', 'LAT'],
        filled=True,
        stroked=False,
        radius_min_pixels=6,
        radius_max_pixels=10,
        line_width_min_pixels=5,
        auto_highlight=True,
        get_radius=50,
        get_line_color=["0+R","0+G","0+B"],
        get_fill_color=["0+R","0+G","0+B"],
        pickable=True)
    
        ###### wholesaler loction
        
        layer_start = pdk.Layer(
        type='ScatterplotLayer',
        data = places_1pd,
        filled=True,
        get_position=['LON', 'LAT'],
        auto_highlight=True,
        radius_min_pixels=6,
        radius_max_pixels=10,
        get_fill_color=[41,181,232],
        pickable=True)

        ###### catchment for orders
        
        isochrone = pdk.Layer(
        "PolygonLayer",
        data2,
        opacity=0.7,
        get_polygon="coordinates",
        filled=True,
        get_line_color=[41,181,232],
        get_fill_color=[200,230,242],
        get_line_width=10,
        line_width_min_pixels=6,
        auto_highlight=True,
        pickable=False)

        ###### journey paths ##
        
        vehicle_paths = pdk.Layer(
        type="PathLayer",
        data=data3,
        pickable=True,
        get_color=["0+R","0+G","0+B"],
        #width_scale=20,
        width_min_pixels=4,
        width_max_pixels=7,
        get_path="coordinates",
        get_width=5)

        view_state = pdk.ViewState(latitude=start_1[1], longitude=start_1[0], zoom=10)

        df = optim_route_line.join(total_time_vehicle,'VEHICLE').select('R','G','B','TOTAL_JOBS','VEHICLE','DURATION').to_pandas()
        
        df['color'] = df.apply(lambda row: f"#{row['R']:02x}{row['G']:02x}{row['B']:02x}", axis=1)
    
        # Create the Altair bar chart
        bars = alt.Chart(df).mark_bar().encode(
        y=alt.Y('VEHICLE', axis=alt.Axis(grid=False)),
        x=alt.X('TOTAL_JOBS', axis=alt.Axis(grid=False,labels=False, title=None)),
        color=alt.Color('color:N', scale=None)  # Use the color column for bar colors
        ).properties(
        width=600,
        height=200
        )

        # Create the text labels for the bars
        text = bars.mark_text(
        align='left',
        baseline='middle',
        dx=3,  # Adjust the dx value to move the text further from the end of the bar
        fontSize=14  # Increase font size
        ).encode(
        text='TOTAL_JOBS',
        x=alt.X('TOTAL_JOBS', axis=alt.Axis(grid=False))
        )

        chart = alt.layer(bars, text).configure_view(
        strokeWidth=0  # Remove the border around the chart
        )


        bars2 = alt.Chart(df).mark_bar().encode(
        y=alt.Y('VEHICLE', axis=alt.Axis(grid=False)),
        x=alt.X('DURATION', axis=alt.Axis(grid=False,labels=False, title=None)),
        color=alt.Color('color:N', scale=None)  # Use the color column for bar colors
        ).properties(
        width=600,
        height=200
        )

        # Create the text labels for the bars
        text2 = bars2.mark_text(
        align='left',
        baseline='middle',
        dx=3,  # Adjust the dx value to move the text further from the end of the bar
        fontSize=14  # Increase font size
        ).encode(
        text='DURATION',
        x=alt.X('DURATION', axis=alt.Axis(grid=False))
        )

        duration_chart = alt.layer(bars2, text2).configure_view(
        strokeWidth=0  # Remove the border around the chart
        )
        st.divider()


        tooltip = {
        "html": """<b>DETAILS</b><BR>{NAME}""",
        "style": {
       "width":"50%",
        "backgroundColor": "steelblue",
        "color": "white",
       "text-wrap": "balance"
            }
        }
    
    
        tab1,tab2,tab3,tab4 = st.tabs(['Map','Vehicle 1','Vehicle2','Vehicle3'])
        with tab1:
            col1,col2 = st.columns(2)
            with col1:
                # Display the chart in Streamlit
                st.markdown('#### TOTAL JOBS BY VEHICLE')
                st.altair_chart(chart, use_container_width=True)

                st.markdown('#### TOTAL DURATION IN MINUTES BY VEHICLE')
                st.altair_chart(duration_chart, use_container_width=True)
            with col2:
                st.markdown('#### MAP OF THE ROUTE WITHIN THE ALLOWED CATCHMENT')
                st.pydeck_chart(pdk.Deck(layers=[isochrone,vehicle_paths,layer_start,layer_end],map_style=None,initial_view_state=view_state,height=900, tooltip=tooltip))
    
        directions = optim_line.select('VEHICLE',col('DIRECTIONS')['features'][0]['properties']['segments'].alias('SEGMENTS'))
    
        directions = directions.join_table_function('flatten',col('SEGMENTS')).select('VEHICLE',col('VALUE').alias('SEGMENT'))

    

        directions = directions.select(col('VEHICLE'),col('SEGMENT')['distance'].alias('"Distance"'),
                                  col('SEGMENT')['duration'].alias('"Duration"'),
                                  col('SEGMENT')['steps'].alias('Steps'))

        directions = directions.join_table_function('flatten',col('STEPS'))\
        .select(col('VEHICLE').alias('"Vehicle"'),col('SEQ'),col('VALUE')['distance'].astype(FloatType()).alias('"Distance"'),
                                                time_from_parts(0,0,col('VALUE')['duration'].astype(FloatType())).alias('"Duration"'),
                                                col('VALUE')['exit_number'].alias('"Exit Number"'),
                                                col('VALUE')['instruction'].astype(StringType()).alias('"Instruction"'),
                                                col('VALUE')['name'].astype(StringType()).alias('"Name"')
                                               ).sort('"Vehicle"','SEQ',col('"Duration"').desc()).cache_result()
                   
            
        def veh_journey(dataframe,vehicle):
            vehicle_1_path = pdk.Layer(
            type="PathLayer",
            data=dataframe[dataframe['VEHICLE']==vehicle],
            pickable=True,
            get_color=["0+R","0+G","0+B"],
            width_scale=20,
            width_min_pixels=4,
            width_max_pixels=7,
            get_path="coordinates",
            get_width=5)
            return vehicle_1_path

        def vehicle_drops(dataframe,vehicle):
            layer_end_v1 = pdk.Layer(
            'ScatterplotLayer',
            dataframe[dataframe['VEHICLE']==vehicle],
            get_position=['LON', 'LAT'],
            filled=True,
            stroked=False,
            radius_min_pixels=6,
            radius_max_pixels=10,
            line_width_min_pixels=5,
            auto_highlight=True,
            get_radius=50,
            get_line_color=["0+R","0+G","0+B"],
            get_fill_color=["0+R","0+G","0+B"],
            pickable=True)
            return layer_end_v1
            
            
        with tab2:
            st.markdown('##### Vehicle 1 Journey')   
            l1 = veh_journey(data3,'1')
            l2 = vehicle_drops(job_detspd,'1')
            st.pydeck_chart(pdk.Deck(layers=[l1,l2,layer_start],map_style=None,initial_view_state=view_state,height=900, tooltip=tooltip))

            
            job = 0
            d1 = directions.filter(col('"Vehicle"')==1)
            for a in d1.select('SEQ').distinct().order_by('SEQ').to_pandas().SEQ:
                job = job+1
                st.markdown(f'''<h5 class="tabheading">Segment{job}</h5>''', unsafe_allow_html=True)
                st.dataframe(d1.filter(col('SEQ')==a).drop('SEQ'))
    
        with tab3:

            st.markdown('##### Vehicle 2 Journey')   
            l1 = veh_journey(data3,'2')
            l2 = vehicle_drops(job_detspd,'2')
            st.pydeck_chart(pdk.Deck(layers=[l1,l2,layer_start],map_style=None,initial_view_state=view_state,height=900, tooltip=tooltip))
            
            job = 0
            d1 = directions.filter(col('"Vehicle"')==2)
            for a in d1.select('SEQ').distinct().order_by('SEQ').to_pandas().SEQ:
                job = job+1
                st.markdown(f'''<h5 class="tabheading">Segment{job}</h5>''', unsafe_allow_html=True)
                st.dataframe(d1.filter(col('SEQ')==a).drop('SEQ'))
                
        with tab4:
            st.markdown('##### Vehicle 3 Journey')   
            l1 = veh_journey(data3,'3')
            l2 = vehicle_drops(job_detspd,'3')
            st.pydeck_chart(pdk.Deck(layers=[l1,l2,layer_start],map_style=None,initial_view_state=view_state,height=900, tooltip=tooltip))
            
            job = 0
            d1 = directions.filter(col('"Vehicle"')==3)
            for a in d1.select('SEQ').distinct().order_by('SEQ').to_pandas().SEQ:
                job = job+1
                st.markdown(f'''<h5 class="tabheading">Segment{job}</h5>''', unsafe_allow_html=True)
                st.dataframe(d1.filter(col('SEQ')==a).drop('SEQ'))
    except:
        st.info('No Shipments Found')