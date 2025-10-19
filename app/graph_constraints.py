#app.graph_constraints.py
async def create_constraints(session):
    #Field
    await session.run("""
        CREATE CONSTRAINT field_id_unique IF NOT EXISTS
        FOR (f:Field) REQUIRE f.field_id IS UNIQUE;
    """)
    #Crop
    await session.run("""
        CREATE CONSTRAINT crop_name_unique IF NOT EXISTS
        FOR (c:Crop) REQUIRE c.name IS UNIQUE;
    """)
    #Station
    await session.run("""
        CREATE CONSTRAINT station_serial_unique IF NOT EXISTS
        FOR (s:Station) REQUIRE s.serial_number IS UNIQUE;
    """)
    #Weather Forecast
    await session.run("""
        CREATE CONSTRAINT weather_forecast_unique IF NOT EXISTS
        FOR (wf:WeatherForecast)
        REQUIRE (wf.station_serial, wf.date) IS UNIQUE;
    """)
    #Weather Day
    await session.run("""
        CREATE CONSTRAINT weather_day_unique IF NOT EXISTS
        FOR (wd:WeatherDay)
        REQUIRE (wd.station_serial, wd.date) IS UNIQUE;
    """)
    #Soil Day
    await session.run("""
        CREATE CONSTRAINT soil_day_unique IF NOT EXISTS
        FOR (sd:SoilDay)
        REQUIRE (sd.station_serial, sd.date) IS UNIQUE;
    """)
    #Soil Layer
    await session.run("""
        CREATE CONSTRAINT soil_layer_unique IF NOT EXISTS
        FOR (sl:SoilLayerReading)
        REQUIRE (sl.station_serial, sl.date, sl.depth_cm) IS UNIQUE;
    """)
    # IrrigationEvent
    await session.run("""
        CREATE CONSTRAINT irrigation_event_unique IF NOT EXISTS 
        FOR (ie:IrrigationEvent) 
        REQUIRE (ie.station_serial, ie.start_datetime) IS UNIQUE
    """)
    # ET0Day
    await session.run("""
        CREATE CONSTRAINT et0day_unique IF NOT EXISTS 
        FOR (n:ET0Day) REQUIRE (n.station_serial, n.date) IS UNIQUE
    """)
    # SAPAnalysis
    await session.run("""
        CREATE CONSTRAINT sap_analysis_unique IF NOT EXISTS
        FOR (sa:SAPAnalysis)
        REQUIRE (sa.field_id, sa.date, sa.crop_name, sa.leaf_type, sa.sample_id) IS UNIQUE
    """)
    # SAPElementResult
    await session.run("""
        CREATE CONSTRAINT sap_element_result_unique IF NOT EXISTS
        FOR (ser:SAPElementResult)
        REQUIRE (ser.sap_analysis_id, ser.nutrient) IS UNIQUE;
    """)
    # OptimumSAPRange
    await session.run("""
        CREATE CONSTRAINT optimum_sap_range_unique IF NOT EXISTS
        FOR (n:OptimumSAPRange)
        REQUIRE (n.crop_name, n.date) IS UNIQUE;
    """)
    # OptimumElementRange
    await session.run("""
        CREATE CONSTRAINT optimum_element_range_unique IF NOT EXISTS
        FOR (n:OptimumElementRange)
        REQUIRE (n.opt_range_id, n.nutrient) IS UNIQUE;
    """)
    # HaneyAnalysis
    await session.run("""
        CREATE CONSTRAINT haney_analysis_unique IF NOT EXISTS
        FOR (n:HaneyAnalysis)
        REQUIRE (n.field_id, n.date, n.lab_no) IS UNIQUE;""")
    # TNDAnalysis
    await session.run("""
        CREATE CONSTRAINT tnd_analysis_unique IF NOT EXISTS
        FOR (n:TNDAnalysis) REQUIRE (n.field_id, n.date, n.lab_no) IS UNIQUE
    """)
    # SoilAnalysis
    await session.run("""
        CREATE CONSTRAINT soil_analysis_unique IF NOT EXISTS
        FOR (n:SoilAnalysis) REQUIRE (n.field_id, n.date, n.lab_id) IS UNIQUE
    """)
    # SoilParamResult
    await session.run("""
        CREATE CONSTRAINT soil_param_result_unique IF NOT EXISTS
        FOR (n:SoilParamResult)
        REQUIRE (n.soil_analysis_id, n.parameter_english) IS UNIQUE
    """)
    # WaterAnalysis
    await session.run("""
        CREATE CONSTRAINT water_analysis_unique IF NOT EXISTS
        FOR (n:WaterAnalysis)
        REQUIRE (n.field_id, n.date, n.sample_source) IS UNIQUE
    """)
    # WaterParamResult
    await session.run("""
        CREATE CONSTRAINT water_param_result_unique IF NOT EXISTS
        FOR (n:WaterParamResult)
        REQUIRE (n.water_analysis_id, n.parameter) IS UNIQUE
    """)
    # ApplicationEvent
    await session.run("""
        CREATE CONSTRAINT application_event_unique IF NOT EXISTS
        FOR (n:ApplicationEvent)
        REQUIRE (n.field_id, n.date, n.crop_name, n.app_type, n.idx) IS UNIQUE
    """)
    # ProductApplication: hızlı MATCH (unique zaten var diye varsayıyoruz)
    await session.run("""
        CREATE CONSTRAINT product_application_unique IF NOT EXISTS
        FOR (n:ProductApplication)
        REQUIRE (n.application_event_id, n.idx) IS UNIQUE
    """)
    # FertilizerProduct: hızlı MATCH
    await session.run("""
        CREATE CONSTRAINT fertilizer_product_unique IF NOT EXISTS
        FOR (n:FertilizerProduct)
        REQUIRE (n.name, n.brand) IS UNIQUE
    """)
    # AppNutrientContent: benzersiz
    await session.run("""
        CREATE CONSTRAINT app_nutrient_content_unique IF NOT EXISTS
        FOR (n:AppNutrientContent)
        REQUIRE (n.product_application_id, n.nutrient) IS UNIQUE
    """)





