# Pythonパッケージをインポート
import altair as alt
import streamlit as st
from snowflake.snowpark.context import get_active_session
import requests
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import FloatType, StringType, IntegerType
import pandas as pd
import pydeck as pdk
import json
from snowflake.snowpark.window import Window

# SnowflakeセッションとStreamlitページ設定を初期化
session = get_active_session()
st.set_page_config(layout="wide")

# # カスタムCSSを読み込み
# with open('extra.css') as f:
#     st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# サイドバーロゴを設定
st.logo('logo.svg')

# 業界の参照テーブル
lookup_table = session.table('DATA.LOOKUP')
industry_choices = lookup_table.select('INDUSTRY').to_pandas()['INDUSTRY'].tolist()

with st.sidebar:
    route_functions_option = st.radio('ルーティング関数の処理場所を選択', ['OPENROUTESERVICE_NATIVE_APP', 'VEHICLE_ROUTING_SIMULATOR'])
    selected_industry = st.radio('業界を選択', industry_choices)

    filtered_lookup = lookup_table.filter(col('INDUSTRY') == selected_industry)
    filtered_lookup = filtered_lookup.with_column('IND', array_to_string('IND', lit(',')))
    filtered_lookup = filtered_lookup.with_column('IND2', array_to_string('IND2', lit(',')))

    lookup_pandas = filtered_lookup.to_pandas()

    # 業界固有のパラメータを抽出
    pa_val = lookup_pandas.PA.iloc[0]
    pb_val = lookup_pandas.PB.iloc[0]
    pc_val = lookup_pandas.PC.iloc[0]
    ind_val = lookup_pandas.IND.iloc[0]
    ind2_val = lookup_pandas.IND2.iloc[0]
    customer_types = json.loads(lookup_pandas.CTYPE.iloc[0])
    skill_types = json.loads(lookup_pandas.STYPE.iloc[0])

# メインアプリケーションヘッダー
st.markdown('''
<h0black>ルート最適化 |</h0black><h0blue> シミュレーション</h0blue><BR>
<h1grey>Powered by the Open Route Service Native App</h1grey>
''', unsafe_allow_html=True)

# ルーティングする配送方法を定義
methods = ['driving-car', 'driving-hgv', 'cycling-road']

# 関心地点データを読み込み
places_filtered = session.table('DATA.places')
places_filtered = places_filtered.select(
    'GEOMETRY',
    call_function('ST_X', col('GEOMETRY')).alias('LON'),
    call_function('ST_Y', col('GEOMETRY')).alias('LAT'),
    col('ADDRESS'),
    col('CATEGORY'),
    col('ALTERNATE'),
    col('PHONES'),
    col('NAME'),
    col('GEOMETRY').alias('POINT')
)

# サイドバーのCortex対応マップフィルター
with st.sidebar:
    st.markdown('##### Cortex対応マップフィルター')
    st.info('以下の場所を中心とする緯度と経度を教えてください')
    model_choice = st.selectbox('モデルを選択:', ['reka-flash', 'mistral-large2', 'claude-4-sonnet'], 2)
    place_input = st.text_input('入力', 'The Empire State Building, New York')
    distance_input = st.number_input('距離（KM）', 1, 300, 15)

@st.cache_data
def get_cortex_place_data(place, model, distance):
    """
    Cortexを使用して指定された場所の地理座標を検索します。
    API呼び出しを最小化するために結果をキャッシュします。
    """
    json_template = str({'LAT': 44.3452, 'LON': 2.345, 'DISTANCE': 100})
    cortex_response = session.create_dataframe([{'PLACE': place}]) \
        .with_column('CENTER', call_function('snowflake.cortex.complete', model,
                                             concat(lit('give me the LAT and LON which centers the following place'),
                                                    col('PLACE'),
                                                    lit(f'return 3 values which are as follows LAT and LON with {distance} as DISTANCE'),
                                                    lit('use the following json template'),
                                                    lit(json_template),
                                                    lit('return only json. DO NOT RETURN COMMENTRY OR VERBIAGE'))))

    parsed_response = cortex_response.select(parse_json(replace(replace('CENTER', "```", ''), 'json', '')).alias('CENTER'))
    
    return parsed_response.select(
        col('CENTER')['LAT'].astype(FloatType()).alias('LAT'),
        col('CENTER')['LON'].astype(FloatType()).alias('LON'),
        call_function('ST_ASWKT', call_function('ST_MAKEPOINT', col('LON'), col('LAT'))).alias('POINT'),
        col('CENTER')['DISTANCE'].astype(FloatType()).alias('"DISTANCE1"'),
        lit(0).alias('DISTANCE'),
        lit(place).alias('NAME')
    ).to_pandas()

with st.sidebar:
    bbox_data = get_cortex_place_data(place_input, model_choice, distance_input)
    st.caption(f'Latitude: {bbox_data.LAT.iloc[0]}')
    st.caption(f'Longitude: {bbox_data.LON.iloc[0]}')

    context_layer = pdk.Layer(
        'ScatterplotLayer',
        bbox_data,
        get_position=['LON', 'LAT'],
        filled=True,
        stroked=False,
        radius_min_pixels=6,
        radius_max_pixels=20,
        auto_highlight=True,
        get_fill_color=[41, 181, 232],
        pickable=True
    )
    st.divider()

# Cortex入力に基づいて卸売業者の場所をフィルタリング
places_wholesalers = places_filtered.filter(
    call_function('ST_DWITHIN', places_filtered['GEOMETRY'], to_geography(lit(bbox_data.POINT.iloc[0])), lit(bbox_data.DISTANCE1.iloc[0]) * 1000)
)

# 指定された業界のカテゴリおよび代替フィールドを検索
places_1 = places_wholesalers.filter(expr(f'''search((CATEGORY,ALTERNATE,NAME),'{ind_val}',analyzer=>'DEFAULT_ANALYZER')'''))
places_1 = places_1.filter(expr(f'''search((CATEGORY,ALTERNATE,NAME),'{ind2_val}',analyzer=>'DEFAULT_ANALYZER')''')).cache_result()

# 業界選択に基づいてジョブテンプレートをカスタマイズ
time_slots = session.table('DATA.JOB_TEMPLATE')
pa_slots = time_slots.filter(col('PRODUCT') == 'pa').join(filtered_lookup.select('PA'))
pb_slots = time_slots.filter(col('PRODUCT') == 'pb').join(filtered_lookup.select('PB'))
pc_slots = time_slots.filter(col('PRODUCT') == 'pc').join(filtered_lookup.select('PC'))

time_slots_union = pa_slots.union(pb_slots).union(pc_slots).with_column('PRODUCT', col('PA')).drop('PA')

# 選択されたポイント内に場所が見つからない場合の処理
if places_1.to_pandas().empty:
    st.info(f'指定されたエリア内に{ind_val}が見つかりませんでした。')
else:
    places_1 = places_1.with_column('DISTANCE', call_function('ST_DISTANCE',
                                                             call_function('ST_MAKEPOINT', col('LON'), col('LAT')),
                                                             call_function('ST_MAKEPOINT', lit(bbox_data.LON.iloc[0]), lit(bbox_data.LAT.iloc[0]))))
    places_1 = places_1.with_column('DISTANCE', round(col('DISTANCE') / 1000, 2)).order_by('DISTANCE')

    @st.cache_data
    def get_cached_places(distance_val, bbox_val, ind_val_param):
        return places_1.to_pandas()

    # 推奨ディストリビューター用のツールチップとpydeckの第2レイヤーを生成
    tooltip = {
        "html": """<b>名前:</b> {NAME} <b><br>距離:</b> {DISTANCE}""",
        "style": {
            "width": "50%",
            "backgroundColor": "steelblue",
            "color": "white",
            "text-wrap": "balance"
        }
    }

    wholesalers_layer = pdk.Layer(
        'ScatterplotLayer',
        get_cached_places(distance_input, bbox_data, ind_val),
        get_position=['LON', 'LAT'],
        filled=True,
        opacity=0.5,
        stroked=False,
        radius_min_pixels=6,
        radius_max_pixels=10,
        auto_highlight=True,
        get_fill_color=[0, 0, 0],
        pickable=True
    )
    st.divider()

    view_state = pdk.ViewState(bbox_data.LON.iloc[0], bbox_data.LAT.iloc[0], zoom=4)

    with st.sidebar:
        @st.cache_data
        def get_warehouses_data(distance_val, bbox_val, ind_val_param):
            return places_1.group_by(col('NAME')).agg(avg('DISTANCE').alias('DISTANCE')).sort(col('DISTANCE').asc()).to_pandas()

        st.caption(f'{distance_input}kmの正方形エリア内の卸売業者')
        st.pydeck_chart(pdk.Deck(layers=[wholesalers_layer, context_layer], map_style=None, initial_view_state=view_state, tooltip=tooltip))

        selected_warehouse = st.selectbox('卸売業者を選択:', get_warehouses_data(distance_input, bbox_data, ind_val))
        selected_customer_types = st.multiselect('顧客タイプを選択:', customer_types, customer_types[0])
        places_1 = places_1.filter(col('NAME') == selected_warehouse).limit(1)

    job_start_lon = places_1.select('LON').collect()[0][0]
    job_start_lat = places_1.select('LAT').collect()[0][0]

    warehouse_address = places_1.select(
        col('POINT'),
        col('NAME'),
        col('ADDRESS')['country'].astype(StringType()).alias('COUNTRY'),
        col('ADDRESS')['freeform'].astype(StringType()).alias('ADDRESS_1'),
        col('ADDRESS')['locality'].astype(StringType()).alias('LOCALITY'),
        col('ADDRESS')['postcode'].astype(StringType()).alias('ZIP'),
        col('PHONES').astype(StringType()).alias('PHONE_NUMBER')
    ).to_pandas()

    st.markdown(f'<h1sub>卸売業者 - {warehouse_address.NAME.iloc[0]}</h1sub>', unsafe_allow_html=True)
    st.markdown(f'**住所:** {warehouse_address.ADDRESS_1.iloc[0]}, {warehouse_address.LOCALITY.iloc[0]}, {warehouse_address.ZIP.iloc[0]}')
    st.markdown(f'**電話番号:** {warehouse_address.PHONE_NUMBER.iloc[0]}')

    st.divider()
    st.markdown('<h1sub>商品を配送できる車両</h1sub>', unsafe_allow_html=True)

    # 車両設定
    veh1_skills = [1]
    veh2_skills = [2]
    veh3_skills = [3]

    veh1_capacity = [4]
    veh2_capacity = [6]
    veh3_capacity = [12]

    # 車両1の設定
    st.markdown('<veh1>車両1</veh1>', unsafe_allow_html=True, help=f'''車両1 {skill_types[veh1_skills[0]-1]} , 容量 {veh1_capacity[0]}''')
    col1, col2, col3 = st.columns(3)
    with col1:
        start_time_0 = st.number_input('開始時間（時）:', 0, 24, 8, key=1)
    with col2:
        end_time_0 = st.number_input('終了時間（時）:', start_time_0, 24, 17, key=2)
    with col3:
        smethod = st.selectbox('配送方法を選択:', methods, key=5)

    places_vehicles_df = places_1.filter(col('NAME') == selected_warehouse).cache_result()

    vehicle_1 = places_vehicles_df.select(
        object_construct(
            lit('profile'), lit(smethod),
            lit('skills'), lit(veh1_skills),
            lit('id'), lit(1),
            lit('start'), array_construct(col('LON'), col('LAT')),
            lit('end'), array_construct(col('LON'), col('LAT')),
            lit('time_windows'), array_construct(lit(start_time_0 * 60 * 60), lit(end_time_0 * 60 * 60)),
            lit('capacity'), lit(veh1_capacity)
        ).alias('VEHICLE')
    )

    # 車両2の設定
    st.markdown('<veh2>車両2</veh2>', unsafe_allow_html=True, help=f'''車両2 {skill_types[veh2_skills[0]-1]}, 容量 {veh2_capacity[0]}''')
    col4, col5, col6 = st.columns(3)
    with col4:
        start_time_1 = st.number_input('開始時間（時）:', 0, 24, 8, key=3)
    with col5:
        end_time_1 = st.number_input('終了時間（時）:', start_time_1, 24, 17, key=4)
    with col6:
        smethod_1 = st.selectbox('配送方法を選択:', methods, key=6)

    vehicle_2 = places_vehicles_df.select(
        object_construct(
            lit('profile'), lit(smethod_1),
            lit('skills'), lit(veh2_skills),
            lit('id'), lit(2),
            lit('start'), array_construct(col('LON'), col('LAT')),
            lit('end'), array_construct(col('LON'), col('LAT')),
            lit('time_windows'), array_construct(lit(start_time_1 * 60 * 60), lit(end_time_1 * 60 * 60)),
            lit('capacity'), lit(veh2_capacity)
        ).alias('VEHICLE')
    )

    # 車両3の設定
    st.markdown('<veh3>車両3</veh3>', unsafe_allow_html=True, help=f'''車両3 {skill_types[veh3_skills[0]-1]} , 容量 {veh3_capacity[0]}''')
    col7, col8, col9 = st.columns(3)
    with col7:
        start_time_2 = st.number_input('開始時間（時）:', 0, 24, 8, key=8)
    with col8:
        end_time_2 = st.number_input('終了時間（時）:', start_time_1, 24, 17, key=9)
    with col9:
        smethod_2 = st.selectbox('配送方法を選択:', methods, key=10)

    vehicle_3 = places_vehicles_df.select(
        object_construct(
            lit('profile'), lit(smethod_2),
            lit('skills'), lit(veh3_skills),
            lit('id'), lit(3),
            lit('start'), array_construct(col('LON'), col('LAT')),
            lit('end'), array_construct(col('LON'), col('LAT')),
            lit('time_windows'), array_construct(lit(start_time_2 * 60 * 60), lit(end_time_2 * 60 * 60)),
            lit('capacity'), lit(veh3_capacity)
        ).alias('VEHICLE')
    )

    # 車両カラーを追加
    vehicle_1 = vehicle_1.with_column('R', lit(125)).with_column('G', lit(68)).with_column('B', lit(207))
    vehicle_2 = vehicle_2.with_column('R', lit(212)).with_column('G', lit(91)).with_column('B', lit(144))
    vehicle_3 = vehicle_3.with_column('R', lit(255)).with_column('G', lit(159)).with_column('B', lit(54))

    # 車両データフレームを作成
    vehicles_details = vehicle_1.union(vehicle_2).union(vehicle_3).with_column('ID', col('VEHICLE')['id']) \
        .with_column('PROFILE', col('VEHICLE')['profile'].astype(StringType())) \
        .with_column('WINDOW', col('VEHICLE')['time_windows'].astype(StringType()))
    
    vehicles_agg = vehicles_details.select(array_agg('VEHICLE').alias('VEH'))
    vehicles_details = vehicles_details.drop('VEHICLE')

    start_coords = places_1.select('LON', 'LAT').collect()[0]

    # 顧客タイプに基づいて場所をフィルタリング
    places_customers = places_filtered.filter(expr(f'''search((CATEGORY,ALTERNATE,NAME),'{" ".join(selected_customer_types)}',analyzer=>'DEFAULT_ANALYZER')'''))

    with st.sidebar:
        range_minutes_input = st.number_input('注文受付エリア時間（分）:', 0, 120, 20)
        based_on_method = st.selectbox('配送方法を選択:', methods)

    # 等時線を計算
    isochrone_df = session.create_dataframe([{'LON': start_coords[0], 'LAT': start_coords[1], 'METHOD': based_on_method, 'RANGE_MINS': range_minutes_input}])
    isochrone_result = isochrone_df.select(call_function(f'{route_functions_option}.CORE.ISOCHRONES',
                                                       (col('METHOD'), col('LON'), col('LAT'), col('RANGE_MINS'))).alias('ISOCHRONE'))
    isochrone_geo = isochrone_result.select(to_geography(col('ISOCHRONE')['features'][0]['geometry']).alias('GEO')).cache_result()

    isochrone_pandas = isochrone_geo.select('GEO').to_pandas()
    isochrone_pandas["coordinates"] = isochrone_pandas["GEO"].apply(lambda row: json.loads(row)["coordinates"])

    # マップ用のPydeckレイヤー
    point_mark_layer = pdk.Layer(
        "ScatterplotLayer",
        places_vehicles_df.to_pandas(),
        get_position=["LON", "LAT"],
        get_fill_color=[255, 0, 0, 200],
        get_radius=300,
        pickable=True,
        auto_highlight=True,
    )

    map_view_state = pdk.ViewState(start_coords[0], start_coords[1], zoom=10)

    isochrone_layer = pdk.Layer(
        "PolygonLayer",
        isochrone_pandas,
        opacity=0.7,
        get_polygon="coordinates",
        filled=True,
        get_line_color=[41, 181, 232],
        get_fill_color=[200, 230, 242],
        get_line_width=10,
        line_width_min_pixels=6,
        auto_highlight=True,
        pickable=False
    )

    with st.sidebar:
        st.caption('選択された卸売業者と等時線')
        st.pydeck_chart(pdk.Deck(layers=[isochrone_layer, point_mark_layer], map_style=None, initial_view_state=map_view_state, tooltip=tooltip))

    submit_button = st.button('ジョブルートを取得')

    if submit_button:
        with st.container():
            # 等時線内の顧客をフィルタリングし、利用可能な時間枠に制限
            places_customers_within_isochrone = places_customers.join(isochrone_geo, call_function('ST_WITHIN', places_customers['POINT'], isochrone_geo['GEO'])) \
                .sample(0.5).limit(time_slots_union.count()).cache_result()

            window_spec = Window.order_by(random())
            places_customers_within_isochrone = places_customers_within_isochrone.with_column('ID', row_number().over(window_spec))
            places_customers_within_isochrone = places_customers_within_isochrone.join(time_slots_union, 'ID')

            # 表示用のジョブ詳細を準備
            places_2_table_display = places_customers_within_isochrone.select(
                'ID',
                col('PRODUCT').alias('"Product"'),
                col('SLOT_START').alias('"Slot Start"'),
                col('SLOT_END').alias('"Slot End"'),
                col('NAME').alias('Name'),
                col('CATEGORY').alias('"Category"'),
                col('ADDRESS')['freeform'].astype(StringType()).alias('"Address"'),
                col('ADDRESS')['locality'].astype(StringType()).alias('"Locality"'),
                col('ADDRESS')['postcode'].astype(StringType()).alias('"Postcode"'),
                col('ADDRESS')['region'].astype(StringType()).alias('"Region"'),
                col('PHONES').astype(StringType()).alias('"Phone Number"')
            )

            # 最適化用のジョブデータを準備
            jobs_data = places_customers_within_isochrone.with_column('JOB', object_construct(
                lit('id'), col('ID'),
                lit('capacity'), lit([2]),
                lit('skills'), array_construct(col('SKILLS')),
                lit('time_window'), array_construct(col('SLOT_START') * 60 * 60, col('SLOT_END') * 60 * 60),
                lit('location'), array_construct(col('LON'), col('LAT'))
            ))
            jobs_agg = jobs_data.select(array_agg('JOB').alias('JOB'))

            # 最適化を実行
            optimization_result = jobs_agg.join(vehicles_agg).select(
                'JOB', 'VEH',
                call_function(f'{route_functions_option}.CORE.OPTIMIZATION', col('JOB'), col('VEH')).alias('OPTIMIZATION')
            )

            optimization_result = optimization_result.with_column('CODES', col('OPTIMIZATION')['codes']) \
                .with_column('ROUTES', col('OPTIMIZATION')['routes']) \
                .with_column('SUMMARY', col('OPTIMIZATION')['summary']) \
                .with_column('UNASSIGNED', col('OPTIMIZATION')['unassigned']) \
                .with_column('COST', col('SUMMARY')['cost']) \
                .with_column('DURATION', col('SUMMARY')['duration']) \
                .with_column('NUMBER_OF_ROUTES', col('SUMMARY')['routes']).drop('SUMMARY')

            optimization_routes = optimization_result.join_table_function('flatten', col('ROUTES')).select('VALUE')

            optimization_details = optimization_routes.select(
                col('VALUE')['amount'].alias('AMOUNT'),
                col('VALUE')['geometry'].alias('GEOMETRY'),
                col('VALUE')['vehicle'].alias('VEHICLE'),
                col('VALUE')['duration'].alias('DURATION'),
                col('VALUE')['steps'].alias('STEPS'),
                col('VALUE')['location'][0].alias('LON'),
                col('VALUE')['location'][1].alias('LAT') # 修正：LATにはインデックス1を使用
            ).cache_result()

            optimization_steps = optimization_details.join_table_function('flatten', col('STEPS'))

            total_time_per_vehicle = optimization_steps.select('VEHICLE', col('VALUE')['duration'].astype(IntegerType()).alias('DURATION')) \
                .filter(col('VALUE')['type'] == 'end')
            total_time_per_vehicle = total_time_per_vehicle.with_column('DURATION', round(div0(col('DURATION'), lit(60)), 2))

            optimization_steps = optimization_steps.select(
                'VEHICLE',
                col('VALUE')['duration'].astype(IntegerType()).alias('DURATION'),
                col('VALUE')['location'][0].astype(FloatType()).alias('LON'),
                col('VALUE')['location'][1].astype(FloatType()).alias('LAT'),
                col('VALUE')['location'].alias('LOCATION'),
                col('VALUE')['type'].astype(StringType()).alias('TYPE'),
                col('VALUE')['job'].alias('JOB')
            )

            optimization_steps_ordered = optimization_steps.sort(col('VEHICLE'), col('DURATION').asc())

            optimization_lines = optimization_steps_ordered.group_by('VEHICLE') \
                .agg(array_agg(col('LOCATION')).within_group(col('DURATION').asc()).alias('LINE'),
                     count('*').alias('TOTAL_JOBS')).with_column('TOTAL_JOBS', col('TOTAL_JOBS') - 2)

            st.markdown('<h1sub>ジョブ詳細</h1sub>', unsafe_allow_html=True)
            job_details_df = optimization_steps.join(places_2_table_display, optimization_steps['JOB'] == places_2_table_display['ID']).drop('LOCATION', 'ID')
            job_details_df = job_details_df.join(vehicles_details, vehicles_details['ID'] == job_details_df['VEHICLE'])

            job_details_df = job_details_df.with_column('TIME', parse_json('WINDOW')[0].astype(IntegerType()))
            job_details_df = job_details_df.with_column('DURATION', cast(col('DURATION'), IntegerType()))
            job_details_df = job_details_df.with_column('TIME', time_from_parts(0, 0, col('TIME') + col('DURATION')))

            job_details_display = job_details_df.select(
                'VEHICLE', 'PROFILE', 'TIME',
                time_from_parts('"Slot End"', 0, 0).alias('"Agreed Time"'),
                time_from_parts(0, 0, 'DURATION').alias('"Cumulative Duration"'),
                'JOB', '"Product"', 'NAME', '"Category"', '"Address"', '"Locality"', '"Postcode"', '"Region"',
                '"Phone Number"', 'R', 'G', 'B', 'ID', 'LAT', 'LON'
            )

            def highlight_first_column(row):
                color = f'background-color: rgb({row["R"]}, {row["G"]}, {row["B"]}); color: white'
                return [color] + [''] * (len(row) - 1)

            df_for_display = job_details_display.drop('LON', 'LAT').to_pandas()
            styled_df = df_for_display.style.apply(highlight_first_column, axis=1)
            st.dataframe(styled_df)

            optimization_lines_with_vehicle_details = optimization_lines.join(vehicles_details, vehicles_details['ID'] == optimization_lines['VEHICLE'])

            optimization_lines_final = optimization_lines_with_vehicle_details.select(
                'VEHICLE', 'R', 'G', 'B', 'PROFILE', 'ID', 'TOTAL_JOBS',
                call_function(f'{route_functions_option}.CORE.DIRECTIONS',
                              col('PROFILE'), object_construct(lit('coordinates'), col('LINE'))).alias('DIRECTIONS')
            ).cache_result()

            if optimization_details.select(col('GEOMETRY')).collect()[0][0] is None:
                optimized_route_geometry = optimization_lines_final.with_column('GEO', col('DIRECTIONS')['features'][0]['geometry'])
            else:
                optimized_route_geometry = optimization_details.select('VEHICLE', col('GEOMETRY').alias('GEO'))
                optimized_route_geometry = optimized_route_geometry.with_column('GEO', object_construct(lit('coordinates'), col('GEO')))
                optimized_route_geometry = optimized_route_geometry.join(vehicles_details, vehicles_details['ID'] == optimized_route_geometry['VEHICLE'])

            optimized_route_geometry = optimized_route_geometry.with_column('NAME', concat(lit('Vehicle '), col('VEHICLE'), lit(' '), col('PROFILE')))
            optimized_route_line = optimization_lines_final.with_column('NAME', concat(lit('Vehicle '), col('VEHICLE'), lit(' '), col('PROFILE')))

            data_for_map = optimized_route_geometry.select('GEO', 'PROFILE', 'NAME', 'VEHICLE', 'ID', 'R', 'G', 'B').to_pandas()
            data_for_map["coordinates"] = data_for_map["GEO"].apply(lambda row: json.loads(row)["coordinates"])

            job_details_pandas = job_details_display.with_column('NAME',
                                                                concat(lit('<b>'), col('"Category"'), lit(':</b>'),
                                                                       col('NAME'), lit('<BR>'),
                                                                       lit('<b>Address:</b> '), col('"Address"'), lit('<BR>'),
                                                                       lit('<b>Postcode:</b> '), col('"Postcode"'), lit('<BR>'),
                                                                       lit('<b>Phone Number:</b> '), col('"Phone Number"'), lit('<BR>'),
                                                                       lit('<b>Cumulate Duration:</b> '), col('"Cumulative Duration"').astype(StringType()))).drop('TIME', '"Agreed Time"', '"Cumulative Duration"').to_pandas()
            places_1_pandas = places_1.to_pandas()

            # 車両配送地点用のPydeckレイヤー
            layer_end = pdk.Layer(
                'ScatterplotLayer',
                job_details_pandas,
                get_position=['LON', 'LAT'],
                filled=True,
                stroked=False,
                radius_min_pixels=6,
                radius_max_pixels=10,
                line_width_min_pixels=5,
                auto_highlight=True,
                get_radius=50,
                get_line_color=["0+R", "0+G", "0+B"],
                get_fill_color=["0+R", "0+G", "0+B"],
                pickable=True
            )

            # 卸売業者の場所用のPydeckレイヤー
            layer_start = pdk.Layer(
                type='ScatterplotLayer',
                data=places_1_pandas,
                filled=True,
                get_position=['LON', 'LAT'],
                auto_highlight=True,
                radius_min_pixels=6,
                radius_max_pixels=10,
                get_fill_color=[41, 181, 232],
                pickable=True
            )

            # 経路パス用のPydeckレイヤー
            vehicle_paths_layer = pdk.Layer(
                type="PathLayer",
                data=data_for_map,
                pickable=True,
                get_color=["0+R", "0+G", "0+B"],
                width_min_pixels=4,
                width_max_pixels=7,
                get_path="coordinates",
                get_width=5
            )

            # Altairチャート
            df_for_charts = optimized_route_line.join(total_time_per_vehicle, 'VEHICLE') \
                .select('R', 'G', 'B', 'TOTAL_JOBS', 'VEHICLE', 'DURATION').to_pandas()
            df_for_charts['color'] = df_for_charts.apply(lambda row: f"#{row['R']:02x}{row['G']:02x}{row['B']:02x}", axis=1)

            # 車両別総ジョブ数のチャート
            jobs_chart_bars = alt.Chart(df_for_charts).mark_bar().encode(
                y=alt.Y('VEHICLE', axis=alt.Axis(grid=False)),
                x=alt.X('TOTAL_JOBS', axis=alt.Axis(grid=False, labels=False, title=None)),
                color=alt.Color('color:N', scale=None)
            ).properties(width=600, height=200)

            jobs_chart_text = jobs_chart_bars.mark_text(
                align='left', baseline='middle', dx=3, fontSize=14
            ).encode(text='TOTAL_JOBS', x=alt.X('TOTAL_JOBS', axis=alt.Axis(grid=False)))
            total_jobs_chart = alt.layer(jobs_chart_bars, jobs_chart_text).configure_view(strokeWidth=0)

            # 車両別総所要時間（分）のチャート
            duration_chart_bars = alt.Chart(df_for_charts).mark_bar().encode(
                y=alt.Y('VEHICLE', axis=alt.Axis(grid=False)),
                x=alt.X('DURATION', axis=alt.Axis(grid=False, labels=False, title=None)),
                color=alt.Color('color:N', scale=None)
            ).properties(width=600, height=200)

            duration_chart_text = duration_chart_bars.mark_text(
                align='left', baseline='middle', dx=3, fontSize=14
            ).encode(text='DURATION', x=alt.X('DURATION', axis=alt.Axis(grid=False)))
            total_duration_chart = alt.layer(duration_chart_bars, duration_chart_text).configure_view(strokeWidth=0)

            st.divider()

            tooltip_map = {
                "html": """<b>詳細</b><BR>{NAME}""",
                "style": {
                    "width": "50%",
                    "backgroundColor": "steelblue",
                    "color": "white",
                    "text-wrap": "balance"
                }
            }

            tab1, tab2, tab3, tab4 = st.tabs(['マップ', '車両1', '車両2', '車両3'])

            with tab1:
                col_map_charts, col_map_display = st.columns(2)
                with col_map_charts:
                    st.markdown('<h1sub>車両別ジョブ数</h1sub>', unsafe_allow_html=True)
                    st.altair_chart(total_jobs_chart, use_container_width=True)
                    st.markdown('<h1sub>車両別総所要時間（分）</h1sub>', unsafe_allow_html=True)
                    st.altair_chart(total_duration_chart, use_container_width=True)
                with col_map_display:
                    st.markdown('<h1sub>許容範囲内のルートのマップ</h1sub>', unsafe_allow_html=True)
                    st.pydeck_chart(pdk.Deck(layers=[isochrone_layer, vehicle_paths_layer, layer_start, layer_end],
                                             map_style=None, initial_view_state=map_view_state, height=900, tooltip=tooltip_map))

            # 各車両の方向案内
            directions_df = optimization_lines_final.select('VEHICLE', col('DIRECTIONS')['features'][0]['properties']['segments'].alias('SEGMENTS'))
            directions_df = directions_df.join_table_function('flatten', col('SEGMENTS')).select('VEHICLE', col('VALUE').alias('SEGMENT'))
            directions_df = directions_df.select(
                col('VEHICLE'),
                col('SEGMENT')['distance'].alias('"Distance"'),
                col('SEGMENT')['duration'].alias('"Duration"'),
                col('SEGMENT')['steps'].alias('Steps')
            )
            directions_df = directions_df.join_table_function('flatten', col('STEPS')) \
                .select(
                col('VEHICLE').alias('"Vehicle"'), col('SEQ'), col('VALUE')['distance'].astype(FloatType()).alias('"Distance"'),
                time_from_parts(0, 0, col('VALUE')['duration'].astype(FloatType())).alias('"Duration"'),
                col('VALUE')['exit_number'].alias('"Exit Number"'),
                col('VALUE')['instruction'].astype(StringType()).alias('"Instruction"'),
                col('VALUE')['name'].astype(StringType()).alias('"Name"')
            ).sort('"Vehicle"', 'SEQ', col('"Duration"').desc()).cache_result()

            def display_vehicle_journey(dataframe_for_map, dataframe_for_drops, vehicle_id, start_layer, view_state):
                """車両固有のマップと指示を表示するヘルパー関数。"""
                vehicle_path_layer = pdk.Layer(
                    type="PathLayer",
                    data=dataframe_for_map[dataframe_for_map['VEHICLE'] == vehicle_id],
                    pickable=True,
                    get_color=["0+R", "0+G", "0+B"],
                    width_scale=20,
                    width_min_pixels=4,
                    width_max_pixels=7,
                    get_path="coordinates",
                    get_width=5
                )
                vehicle_drops_layer = pdk.Layer(
                    'ScatterplotLayer',
                    dataframe_for_drops[dataframe_for_drops['VEHICLE'] == vehicle_id],
                    get_position=['LON', 'LAT'],
                    filled=True,
                    stroked=False,
                    radius_min_pixels=6,
                    radius_max_pixels=10,
                    line_width_min_pixels=5,
                    auto_highlight=True,
                    get_radius=50,
                    get_line_color=["0+R", "0+G", "0+B"],
                    get_fill_color=["0+R", "0+G", "0+B"],
                    pickable=True
                )
                st.pydeck_chart(pdk.Deck(layers=[vehicle_path_layer, vehicle_drops_layer, start_layer],
                                         map_style=None, initial_view_state=view_state, height=900, tooltip=tooltip_map))

                segment_job_counter = 0
                vehicle_directions = directions_df.filter(col('"Vehicle"') == vehicle_id)
                for segment_seq in vehicle_directions.select('SEQ').distinct().order_by('SEQ').to_pandas().SEQ:
                    segment_job_counter += 1
                    st.markdown(f'''<h1sub>Segment {segment_job_counter}</h1sub>''', unsafe_allow_html=True)
                    st.dataframe(vehicle_directions.filter(col('SEQ') == segment_seq).drop('SEQ'))

            with tab2:
                st.markdown('<h1sub>車両1の経路</h1sub>', unsafe_allow_html=True)
                display_vehicle_journey(data_for_map, job_details_pandas, '1', layer_start, map_view_state)

            with tab3:
                st.markdown('<h1sub>車両2の経路</h1sub>', unsafe_allow_html=True)
                display_vehicle_journey(data_for_map, job_details_pandas, '2', layer_start, map_view_state)

            with tab4:
                st.markdown('<h1sub>車両3の経路</h1sub>', unsafe_allow_html=True)
                display_vehicle_journey(data_for_map, job_details_pandas, '3', layer_start, map_view_state)