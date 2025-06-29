class QueryController :

    def __init__(self):
        pass

    @staticmethod
    def get_property_query(filter):
        query = f'''
                Select  
                fid,
                ids_market_data_spot2,
                ids_market_data_inmuebles24,
                ids_market_data_propiedades,
                street_address, 
                centroid,
                is_on_market,
                total_surface_area,
                total_construction_area,
                year_built,
                special_facilities,
                unit_land_value,
                land_value,
                key_vus,
                predominant_level,
                total_houses,
                locality_size,
                floor_levels,
                h3_indexes,
                open_space,
                id_land_use,
                id_municipality,
                id_city_blocks,
                rent_price_spot2,
                rent_price_per_m2_spot2,
                buy_price_spot2,
                buy_price_per_m2_spot2,
                total_area_spot2,
                property_type_spot2,
                rent_price_inmuebles24,
                rent_price_per_m2_inmuebles24,
                buy_price_inmuebles24,
                buy_price_per_m2_inmuebles24,
                total_area_inmuebles24,
                property_type_inmuebles24,
                rent_price_propiedades,
                rent_price_per_m2_propiedades,
                buy_price_propiedades,
                buy_price_per_m2_propiedades,
                total_area_propiedades,
                block_type,
                density_d,
                usage_desc,
                city_link,
                scope,
                height,
                cos,
                cus,
                total_built_perm,
                property_count_per_lot,
                min_housing,
                ids_market_data_inmuebles24,
                crecimiento_promedio_municipal,
                crecimiento_promedio_entidad,
                crecimiento_promedio_ageb,
                brands_active_life_front,
                brands_arts_and_entertainment_front,
                brands_attractions_and_activities_front,
                brands_automotive_front,
                brands_eat_and_drink_front,
                brands_education_front,
                brands_financial_service_front,
                brands_health_and_medical_front,
                brands_public_service_and_government_front,
                brands_retail_front,
                brands_active_life_500m,
                brands_arts_and_entertainment_500m,
                brands_attractions_and_activities_500m,
                brands_automotive_500m,
                brands_eat_and_drink_500m,
                brands_education_500m,
                brands_financial_service_500m,
                brands_health_and_medical_500m,
                brands_public_service_and_government_500m,
                brands_retail_500m,
                brands_active_life_1km,
                brands_arts_and_entertainment_1km,
                brands_attractions_and_activities_1km,
                brands_automotive_1km,
                brands_eat_and_drink_1km,
                brands_education_1km,
                brands_financial_service_1km,
                brands_health_and_medical_1km,
                brands_public_service_and_government_1km,
                brands_retail_1km,
                at_rest_avg_x_hour_0_front,
                pedestrian_avg_x_hour_0_front,
                motor_vehicle_avg_x_hour_0_front,
                at_rest_avg_x_hour_1_front,
                pedestrian_avg_x_hour_1_front,
                motor_vehicle_avg_x_hour_1_front,
                at_rest_avg_x_hour_2_front,
                pedestrian_avg_x_hour_2_front,
                motor_vehicle_avg_x_hour_2_front,
                at_rest_avg_x_hour_3_front,
                pedestrian_avg_x_hour_3_front,
                motor_vehicle_avg_x_hour_3_front,
                at_rest_avg_x_hour_4_front,
                pedestrian_avg_x_hour_4_front,
                motor_vehicle_avg_x_hour_4_front,
                at_rest_avg_x_hour_5_front,
                pedestrian_avg_x_hour_5_front,
                motor_vehicle_avg_x_hour_5_front,
                at_rest_avg_x_hour_6_front,
                pedestrian_avg_x_hour_6_front,
                motor_vehicle_avg_x_hour_6_front,
                at_rest_avg_x_hour_7_front,
                pedestrian_avg_x_hour_7_front,
                motor_vehicle_avg_x_hour_7_front,
                at_rest_avg_x_hour_8_front,
                pedestrian_avg_x_hour_8_front,
                motor_vehicle_avg_x_hour_8_front,
                at_rest_avg_x_hour_9_front,
                pedestrian_avg_x_hour_9_front,
                motor_vehicle_avg_x_hour_9_front,
                at_rest_avg_x_hour_10_front,
                pedestrian_avg_x_hour_10_front,
                motor_vehicle_avg_x_hour_10_front,
                at_rest_avg_x_hour_11_front,
                pedestrian_avg_x_hour_11_front,
                motor_vehicle_avg_x_hour_11_front,
                at_rest_avg_x_hour_12_front,
                pedestrian_avg_x_hour_12_front,
                motor_vehicle_avg_x_hour_12_front,
                at_rest_avg_x_hour_13_front,
                pedestrian_avg_x_hour_13_front,
                motor_vehicle_avg_x_hour_13_front,
                at_rest_avg_x_hour_14_front,
                pedestrian_avg_x_hour_14_front,
                motor_vehicle_avg_x_hour_14_front,
                at_rest_avg_x_hour_15_front,
                pedestrian_avg_x_hour_15_front,
                motor_vehicle_avg_x_hour_15_front,
                at_rest_avg_x_hour_16_front,
                pedestrian_avg_x_hour_16_front,
                motor_vehicle_avg_x_hour_16_front,
                at_rest_avg_x_hour_17_front,
                pedestrian_avg_x_hour_17_front,
                motor_vehicle_avg_x_hour_17_front,
                at_rest_avg_x_hour_18_front,
                pedestrian_avg_x_hour_18_front,
                motor_vehicle_avg_x_hour_18_front,
                at_rest_avg_x_hour_19_front,
                pedestrian_avg_x_hour_19_front,
                motor_vehicle_avg_x_hour_19_front,
                at_rest_avg_x_hour_20_front,
                pedestrian_avg_x_hour_20_front,
                motor_vehicle_avg_x_hour_20_front,
                at_rest_avg_x_hour_21_front,
                pedestrian_avg_x_hour_21_front,
                motor_vehicle_avg_x_hour_21_front,
                at_rest_avg_x_hour_22_front,
                pedestrian_avg_x_hour_22_front,
                motor_vehicle_avg_x_hour_22_front,
                at_rest_avg_x_hour_23_front,
                pedestrian_avg_x_hour_23_front,
                motor_vehicle_avg_x_hour_23_front,
                at_rest_avg_x_day_of_week_1_front,
                pedestrian_avg_x_day_of_week_1_front,
                motor_vehicle_avg_x_day_of_week_1_front,
                at_rest_avg_x_day_of_week_2_front,
                pedestrian_avg_x_day_of_week_2_front,
                motor_vehicle_avg_x_day_of_week_2_front,
                at_rest_avg_x_day_of_week_3_front,
                pedestrian_avg_x_day_of_week_3_front,
                motor_vehicle_avg_x_day_of_week_3_front,
                at_rest_avg_x_day_of_week_4_front,
                pedestrian_avg_x_day_of_week_4_front,
                motor_vehicle_avg_x_day_of_week_4_front,
                at_rest_avg_x_day_of_week_5_front,
                pedestrian_avg_x_day_of_week_5_front,
                motor_vehicle_avg_x_day_of_week_5_front,
                at_rest_avg_x_day_of_week_6_front,
                pedestrian_avg_x_day_of_week_6_front,
                motor_vehicle_avg_x_day_of_week_6_front,
                at_rest_avg_x_day_of_week_7_front,
                pedestrian_avg_x_day_of_week_7_front,
                motor_vehicle_avg_x_day_of_week_7_front,
                at_rest_avg_x_day_of_week_1_500m,
                pedestrian_avg_x_day_of_week_1_500m,
                motor_vehicle_avg_x_day_of_week_1_500m,
                at_rest_avg_x_day_of_week_2_500m,
                pedestrian_avg_x_day_of_week_2_500m,
                motor_vehicle_avg_x_day_of_week_2_500m,
                at_rest_avg_x_day_of_week_3_500m,
                pedestrian_avg_x_day_of_week_3_500m,
                motor_vehicle_avg_x_day_of_week_3_500m,
                at_rest_avg_x_day_of_week_4_500m,
                pedestrian_avg_x_day_of_week_4_500m,
                motor_vehicle_avg_x_day_of_week_4_500m,
                at_rest_avg_x_day_of_week_5_500m,
                pedestrian_avg_x_day_of_week_5_500m,
                motor_vehicle_avg_x_day_of_week_5_500m,
                at_rest_avg_x_day_of_week_6_500m,
                pedestrian_avg_x_day_of_week_6_500m,
                motor_vehicle_avg_x_day_of_week_6_500m,
                at_rest_avg_x_day_of_week_7_500m,
                pedestrian_avg_x_day_of_week_7_500m,
                motor_vehicle_avg_x_day_of_week_7_500m,
                at_rest_avg_x_hour_0_500m,
                pedestrian_avg_x_hour_0_500m,
                motor_vehicle_avg_x_hour_0_500m,
                at_rest_avg_x_hour_1_500m,
                pedestrian_avg_x_hour_1_500m,
                motor_vehicle_avg_x_hour_1_500m,
                at_rest_avg_x_hour_2_500m,
                pedestrian_avg_x_hour_2_500m,
                motor_vehicle_avg_x_hour_2_500m,
                at_rest_avg_x_hour_3_500m,
                pedestrian_avg_x_hour_3_500m,
                motor_vehicle_avg_x_hour_3_500m,
                at_rest_avg_x_hour_4_500m,
                pedestrian_avg_x_hour_4_500m,
                motor_vehicle_avg_x_hour_4_500m,
                at_rest_avg_x_hour_5_500m,
                pedestrian_avg_x_hour_5_500m,
                motor_vehicle_avg_x_hour_5_500m,
                at_rest_avg_x_hour_6_500m,
                pedestrian_avg_x_hour_6_500m,
                motor_vehicle_avg_x_hour_6_500m,
                at_rest_avg_x_hour_7_500m,
                pedestrian_avg_x_hour_7_500m,
                motor_vehicle_avg_x_hour_7_500m,
                at_rest_avg_x_hour_8_500m,
                pedestrian_avg_x_hour_8_500m,
                motor_vehicle_avg_x_hour_8_500m,
                at_rest_avg_x_hour_9_500m,
                pedestrian_avg_x_hour_9_500m,
                motor_vehicle_avg_x_hour_9_500m,
                at_rest_avg_x_hour_10_500m,
                pedestrian_avg_x_hour_10_500m,
                motor_vehicle_avg_x_hour_10_500m,
                at_rest_avg_x_hour_11_500m,
                pedestrian_avg_x_hour_11_500m,
                motor_vehicle_avg_x_hour_11_500m,
                at_rest_avg_x_hour_12_500m,
                pedestrian_avg_x_hour_12_500m,
                motor_vehicle_avg_x_hour_12_500m,
                at_rest_avg_x_hour_13_500m,
                pedestrian_avg_x_hour_13_500m,
                motor_vehicle_avg_x_hour_13_500m,
                at_rest_avg_x_hour_14_500m,
                pedestrian_avg_x_hour_14_500m,
                motor_vehicle_avg_x_hour_14_500m,
                at_rest_avg_x_hour_15_500m,
                pedestrian_avg_x_hour_15_500m,
                motor_vehicle_avg_x_hour_15_500m,
                at_rest_avg_x_hour_16_500m,
                pedestrian_avg_x_hour_16_500m,
                motor_vehicle_avg_x_hour_16_500m,
                at_rest_avg_x_hour_17_500m,
                pedestrian_avg_x_hour_17_500m,
                motor_vehicle_avg_x_hour_17_500m,
                at_rest_avg_x_hour_18_500m,
                pedestrian_avg_x_hour_18_500m,
                motor_vehicle_avg_x_hour_18_500m,
                at_rest_avg_x_hour_19_500m,
                pedestrian_avg_x_hour_19_500m,
                motor_vehicle_avg_x_hour_19_500m,
                at_rest_avg_x_hour_20_500m,
                pedestrian_avg_x_hour_20_500m,
                motor_vehicle_avg_x_hour_20_500m,
                at_rest_avg_x_hour_21_500m,
                pedestrian_avg_x_hour_21_500m,
                motor_vehicle_avg_x_hour_21_500m,
                at_rest_avg_x_hour_22_500m,
                pedestrian_avg_x_hour_22_500m,
                motor_vehicle_avg_x_hour_22_500m,
                at_rest_avg_x_hour_23_500m,
                pedestrian_avg_x_hour_23_500m,
                motor_vehicle_avg_x_hour_23_500m
                FROM blackprint_db_prd.data_product.v_parcel_v3 ''' + filter
        return query
    
    @staticmethod
    def get_demographics_query(fid):
        query = f'''select 
                fid,
                neighborhood,
                nom_mun,
                predominant_level,
                ageb_code,
                vivtot, -- total houses
                vivtot_colonia,
                vivtot_alcaldia,
                prom_ocup, -- average household size
                prom_ocup_colonia, 
                prom_ocup_alcaldia,
                pro_ocup_c, -- average number of rooms
                pro_ocup_c_colonia, 
                pro_ocup_c_alcaldia, 
                ses_ab, -- socio-economic level AB
                ses_ab_colonia,
                ses_ab_alcaldia,
                ses_c_plus , -- socio-economic level C+
                ses_c_plus_colonia,
                ses_c_plus_alcaldia,
                ses_c, -- socio-economic level C
                ses_c_colonia,
                ses_c_alcaldia,
                ses_c_minus, -- socio-economic level C-
                ses_c_minus_colonia,
                ses_c_minus_alcaldia,
                ses_d , -- socio-economic level D
                ses_d_colonia,
                ses_d_alcaldia,
                ses_d_plus,  -- socio-economic level D+
                ses_d_plus_colonia,
                ses_d_plus_alcaldia,
                ses_e, -- socio-economic level E
                ses_e_colonia,
                ses_e_alcaldia,
                pobtot, -- total population
                pobtot_colonia,
                pobtot_alcaldia,
                pobmas, -- total male population
                pobmas_colonia,
                pobmas_alcaldia,
                pobfem, -- total female population
                pobfem_colonia,
                pobfem_alcaldia,
                p_3a5, -- total education between 3 and 5 years
                p_3a5_colonia,
                p_3a5_alcaldia,
                p3a5_noa, -- total education between 3 and 5 years attending school
                p3a5_noa_colonia,
                p3a5_noa_alcaldia,
                p_6a11, -- total education between 6 and 11 years
                p_6a11_colonia,
                p_6a11_alcaldia,
                p6a11_noa, -- total education between 6 and 11 years attending school
                p6a11_noa_colonia,
                p6a11_noa_alcaldia,
                p_12a14, -- total education between 12 and 14 years
                p_12a14_colonia,
                p_12a14_alcaldia,
                p12a14noa, -- total education between 12 and 14 years attending school
                p12a14noa_colonia,
                p12a14noa_alcaldia,
                p_15a17, -- total education between 15 and 17 years
                p_15a17_colonia,
                p_15a17_alcaldia,
                p15a17a, -- total education between 15 and 17 years attending school
                p15a17a_colonia,
                p15a17a_alcaldia,
                p_18a24 , -- total education between 18 and 24 years
                p_18a24_colonia,
                p_18a24_alcaldia,
                p18a24a, -- total education between 18 and 24 years attending school
                p18a24a_colonia,
                p18a24a_alcaldia,
                pea, -- total workforce
                pea_colonia,
                pea_alcaldia,
                pea_m,  -- total male workforce
                pea_m_colonia,
                pea_m_alcaldia,
                pea_f,  -- total female workforce
                pea_f_colonia,
                pea_f_alcaldia,
                pe_inac, -- total inactive population
                pe_inac_colonia,
                pe_inac_alcaldia,
                pe_inac_m, -- total inactive male population
                pe_inac_m_colonia,
                pe_inac_m_alcaldia,
                pe_inac_f, -- total inactive female population
                pe_inac_f_colonia,
                pe_inac_f_alcaldia,
                pocupada, -- total employed population
                pocupada_colonia, 
                pocupada_alcaldia,
                pocupada_m, -- total employed male population
                pocupada_m_colonia,
                pocupada_m_alcaldia,
                pocupada_f, -- total employed female population
                pocupada_f_colonia,
                pocupada_f_alcaldia,
                pdesocup, -- total unemployed population
                pdesocup_colonia,
                pdesocup_alcaldia,
                pdesocup_m, -- total unemployed male population
                pdesocup_m_colonia,
                pdesocup_m_alcaldia,
                pdesocup_f, -- total unemployed female population
                pdesocup_f_colonia,
                pdesocup_f_alcaldia,
                -- This is the population count per year at the AGEB level (all cols named pob_20XX_ageb)
                -- And the cambio_porcentual_20XX_ageb ones are the % changes between each period—lets you peep if the pop jumped or dipped between 2000 and 2005
                pob_2000_ageb,
                pob_2005_ageb,
                pob_2010_ageb,
                pob_2015_ageb,
                pob_2020_ageb,
                cambio_porcentual_2005_ageb,
                cambio_porcentual_2010_ageb,
                cambio_porcentual_2015_ageb,
                cambio_porcentual_2020_ageb,
                -- This is the pop count per year at the municipality/alcaldía level (all cols named pob_20XX_municipal)
                -- And the cambio_porcentual_20XX_municipal ones are the % changes between each period—lets you peep if the pop jumped or dipped between 2000 and 2005
                pob_2000_municipal,
                pob_2010_municipal,
                pob_2005_municipal,
                pob_2015_municipal,
                pob_2020_municipal,
                cambio_porcentual_2005_municipal,
                cambio_porcentual_2010_municipal,
                cambio_porcentual_2015_municipal,
                cambio_porcentual_2020_municipal,
                -- This is the pop count per year at the state/entity level (all cols named pob_20XX_entidad)
                -- And the cambio_porcentual_20XX_entidad ones are the % changes between each period—lets you peep if the pop jumped or dipped between 2000 and 2005
                pob_2000_entidad,
                pob_2005_entidad,
                pob_2010_entidad,
                pob_2015_entidad,
                pob_2020_entidad,
                cambio_porcentual_2005_entidad,
                cambio_porcentual_2010_entidad,
                cambio_porcentual_2015_entidad,
                cambio_porcentual_2020_entidad
                from blackprint_db_prd.data_product.v_parcel_v3
                where fid = {fid}
                    '''
        return query
    
    @staticmethod
    def get_commercial_growth_query(fid):
        query = f'''SELECT 
    fid,

    -- AGEB
    total_businesses_2010_ageb,
    total_businesses_2015_ageb,
    economic_growth_2015_ageb,
    total_businesses_2017_ageb,
    economic_growth_2017_ageb,
    total_businesses_2020_ageb,
    economic_growth_2020_ageb,
    total_businesses_2023_ageb,
    economic_growth_2023_ageb,
    
    -- MUNICIPAL
    total_businesses_2010_municipal,
    total_businesses_2015_municipal,
    economic_growth_2015_municipal,
    total_businesses_2017_municipal,
    economic_growth_2017_municipal,
    total_businesses_2020_municipal,
    economic_growth_2020_municipal,
    total_businesses_2023_municipal,
    economic_growth_2023_municipal,
    
    -- ENTIDAD
    total_businesses_2010_entidad,
    total_businesses_2015_entidad,
    economic_growth_2015_entidad,
    total_businesses_2017_entidad,
    economic_growth_2017_entidad,
    total_businesses_2020_entidad,
    economic_growth_2020_entidad,
    total_businesses_2023_entidad,
    economic_growth_2023_entidad,

    -- ==============================================
    -- CATEGORY: EAT_AND_DRINK
    -- ==============================================
    
    -- EAT_AND_DRINK - AGEB
    total_businesses_2010_eat_and_drink_ageb,
    total_businesses_2015_eat_and_drink_ageb,
    economic_growth_2015_eat_and_drink_ageb,
    total_businesses_2017_eat_and_drink_ageb,
    economic_growth_2017_eat_and_drink_ageb,
    total_businesses_2020_eat_and_drink_ageb,
    economic_growth_2020_eat_and_drink_ageb,
    total_businesses_2023_eat_and_drink_ageb,
    economic_growth_2023_eat_and_drink_ageb,
    
    -- EAT_AND_DRINK - MUNICIPAL
    total_businesses_2010_eat_and_drink_municipal,
    total_businesses_2015_eat_and_drink_municipal,
    economic_growth_2015_eat_and_drink_municipal,
    total_businesses_2017_eat_and_drink_municipal,
    economic_growth_2017_eat_and_drink_municipal,
    total_businesses_2020_eat_and_drink_municipal,
    economic_growth_2020_eat_and_drink_municipal,
    total_businesses_2023_eat_and_drink_municipal,
    economic_growth_2023_eat_and_drink_municipal,
    
    -- EAT_AND_DRINK - ENTIDAD
    total_businesses_2010_eat_and_drink_entidad,
    total_businesses_2015_eat_and_drink_entidad,
    economic_growth_2015_eat_and_drink_entidad,
    total_businesses_2017_eat_and_drink_entidad,
    economic_growth_2017_eat_and_drink_entidad,
    total_businesses_2020_eat_and_drink_entidad,
    economic_growth_2020_eat_and_drink_entidad,
    total_businesses_2023_eat_and_drink_entidad,
    economic_growth_2023_eat_and_drink_entidad,
    
    -- ==============================================
    -- CATEGORY: HEALTH_AND_MEDICAL
    -- ==============================================
    
    -- HEALTH_AND_MEDICAL - AGEB
    total_businesses_2010_health_and_medical_ageb,
    total_businesses_2015_health_and_medical_ageb,
    economic_growth_2015_health_and_medical_ageb,
    total_businesses_2017_health_and_medical_ageb,
    economic_growth_2017_health_and_medical_ageb,
    total_businesses_2020_health_and_medical_ageb,
    economic_growth_2020_health_and_medical_ageb,
    total_businesses_2023_health_and_medical_ageb,
    economic_growth_2023_health_and_medical_ageb,
    
    -- HEALTH_AND_MEDICAL - MUNICIPAL
    total_businesses_2010_health_and_medical_municipal,
    total_businesses_2015_health_and_medical_municipal,
    economic_growth_2015_health_and_medical_municipal,
    total_businesses_2017_health_and_medical_municipal,
    economic_growth_2017_health_and_medical_municipal,
    total_businesses_2020_health_and_medical_municipal,
    economic_growth_2020_health_and_medical_municipal,
    total_businesses_2023_health_and_medical_municipal,
    economic_growth_2023_health_and_medical_municipal,
    
    -- HEALTH_AND_MEDICAL - ENTIDAD
    total_businesses_2010_health_and_medical_entidad,
    total_businesses_2015_health_and_medical_entidad,
    economic_growth_2015_health_and_medical_entidad,
    total_businesses_2017_health_and_medical_entidad,
    economic_growth_2017_health_and_medical_entidad,
    total_businesses_2020_health_and_medical_entidad,
    economic_growth_2020_health_and_medical_entidad,
    total_businesses_2023_health_and_medical_entidad,
    economic_growth_2023_health_and_medical_entidad,
    
    -- ==============================================
    -- CATEGORY: BEAUTY_AND_SPA
    -- ==============================================
    
    -- BEAUTY_AND_SPA - AGEB
    total_businesses_2010_beauty_and_spa_ageb,
    total_businesses_2015_beauty_and_spa_ageb,
    economic_growth_2015_beauty_and_spa_ageb,
    total_businesses_2017_beauty_and_spa_ageb,
    economic_growth_2017_beauty_and_spa_ageb,
    total_businesses_2020_beauty_and_spa_ageb,
    economic_growth_2020_beauty_and_spa_ageb,
    total_businesses_2023_beauty_and_spa_ageb,
    economic_growth_2023_beauty_and_spa_ageb,
    
    -- BEAUTY_AND_SPA - MUNICIPAL
    total_businesses_2010_beauty_and_spa_municipal,
    total_businesses_2015_beauty_and_spa_municipal,
    economic_growth_2015_beauty_and_spa_municipal,
    total_businesses_2017_beauty_and_spa_municipal,
    economic_growth_2017_beauty_and_spa_municipal,
    total_businesses_2020_beauty_and_spa_municipal,
    economic_growth_2020_beauty_and_spa_municipal,
    total_businesses_2023_beauty_and_spa_municipal,
    economic_growth_2023_beauty_and_spa_municipal,
    
    -- BEAUTY_AND_SPA - ENTIDAD
    total_businesses_2010_beauty_and_spa_entidad,
    total_businesses_2015_beauty_and_spa_entidad,
    economic_growth_2015_beauty_and_spa_entidad,
    total_businesses_2017_beauty_and_spa_entidad,
    economic_growth_2017_beauty_and_spa_entidad,
    total_businesses_2020_beauty_and_spa_entidad,
    economic_growth_2020_beauty_and_spa_entidad,
    total_businesses_2023_beauty_and_spa_entidad,
    economic_growth_2023_beauty_and_spa_entidad,
    
    -- ==============================================
    -- CATEGORY: FINANCIAL_SERVICE
    -- ==============================================
    
    -- FINANCIAL_SERVICE - AGEB
    total_businesses_2010_financial_service_ageb,
    total_businesses_2015_financial_service_ageb,
    economic_growth_2015_financial_service_ageb,
    total_businesses_2017_financial_service_ageb,
    economic_growth_2017_financial_service_ageb,
    total_businesses_2020_financial_service_ageb,
    economic_growth_2020_financial_service_ageb,
    total_businesses_2023_financial_service_ageb,
    economic_growth_2023_financial_service_ageb,
    
    -- FINANCIAL_SERVICE - MUNICIPAL
    total_businesses_2010_financial_service_municipal,
    total_businesses_2015_financial_service_municipal,
    economic_growth_2015_financial_service_municipal,
    total_businesses_2017_financial_service_municipal,
    economic_growth_2017_financial_service_municipal,
    total_businesses_2020_financial_service_municipal,
    economic_growth_2020_financial_service_municipal,
    total_businesses_2023_financial_service_municipal,
    economic_growth_2023_financial_service_municipal,
    
    -- FINANCIAL_SERVICE - ENTIDAD
    total_businesses_2010_financial_service_entidad,
    total_businesses_2015_financial_service_entidad,
    economic_growth_2015_financial_service_entidad,
    total_businesses_2017_financial_service_entidad,
    economic_growth_2017_financial_service_entidad,
    total_businesses_2020_financial_service_entidad,
    economic_growth_2020_financial_service_entidad,
    total_businesses_2023_financial_service_entidad,
    economic_growth_2023_financial_service_entidad,
    
    -- ==============================================
    -- CATEGORY: ARTS_AND_ENTERTAINMENT
    -- ==============================================
    
    -- ARTS_AND_ENTERTAINMENT - AGEB
    total_businesses_2010_arts_and_entertainment_ageb,
    total_businesses_2015_arts_and_entertainment_ageb,
    economic_growth_2015_arts_and_entertainment_ageb,
    total_businesses_2017_arts_and_entertainment_ageb,
    economic_growth_2017_arts_and_entertainment_ageb,
    total_businesses_2020_arts_and_entertainment_ageb,
    economic_growth_2020_arts_and_entertainment_ageb,
    total_businesses_2023_arts_and_entertainment_ageb,
    economic_growth_2023_arts_and_entertainment_ageb,
    
    -- ARTS_AND_ENTERTAINMENT - MUNICIPAL
    total_businesses_2010_arts_and_entertainment_municipal,
    total_businesses_2015_arts_and_entertainment_municipal,
    economic_growth_2015_arts_and_entertainment_municipal,
    total_businesses_2017_arts_and_entertainment_municipal,
    economic_growth_2017_arts_and_entertainment_municipal,
    total_businesses_2020_arts_and_entertainment_municipal,
    economic_growth_2020_arts_and_entertainment_municipal,
    total_businesses_2023_arts_and_entertainment_municipal,
    economic_growth_2023_arts_and_entertainment_municipal,
    
    -- ARTS_AND_ENTERTAINMENT - ENTIDAD
    total_businesses_2010_arts_and_entertainment_entidad,
    total_businesses_2015_arts_and_entertainment_entidad,
    economic_growth_2015_arts_and_entertainment_entidad,
    total_businesses_2017_arts_and_entertainment_entidad,
    economic_growth_2017_arts_and_entertainment_entidad,
    total_businesses_2020_arts_and_entertainment_entidad,
    economic_growth_2020_arts_and_entertainment_entidad,
    total_businesses_2023_arts_and_entertainment_entidad,
    economic_growth_2023_arts_and_entertainment_entidad,
    
    -- ==============================================
    -- CATEGORY: ACTIVE_LIFE
    -- ==============================================
    
    -- ACTIVE_LIFE - AGEB
    total_businesses_2010_active_life_ageb,
    total_businesses_2015_active_life_ageb,
    economic_growth_2015_active_life_ageb,
    total_businesses_2017_active_life_ageb,
    economic_growth_2017_active_life_ageb,
    total_businesses_2020_active_life_ageb,
    economic_growth_2020_active_life_ageb,
    total_businesses_2023_active_life_ageb,
    economic_growth_2023_active_life_ageb,
    
    -- ACTIVE_LIFE - MUNICIPAL
    total_businesses_2010_active_life_municipal,
    total_businesses_2015_active_life_municipal,
    economic_growth_2015_active_life_municipal,
    total_businesses_2017_active_life_municipal,
    economic_growth_2017_active_life_municipal,
    total_businesses_2020_active_life_municipal,
    economic_growth_2020_active_life_municipal,
    total_businesses_2023_active_life_municipal,
    economic_growth_2023_active_life_municipal,
    
    -- ACTIVE_LIFE - ENTIDAD
    total_businesses_2010_active_life_entidad,
    total_businesses_2015_active_life_entidad,
    economic_growth_2015_active_life_entidad,
    total_businesses_2017_active_life_entidad,
    economic_growth_2017_active_life_entidad,
    total_businesses_2020_active_life_entidad,
    economic_growth_2020_active_life_entidad,
    total_businesses_2023_active_life_entidad,
    economic_growth_2023_active_life_entidad,
    
    -- ==============================================
    -- CATEGORY: RETAIL
    -- ==============================================
    
    -- RETAIL - AGEB
    total_businesses_2010_retail_ageb,
    total_businesses_2015_retail_ageb,
    economic_growth_2015_retail_ageb,
    total_businesses_2017_retail_ageb,
    economic_growth_2017_retail_ageb,
    total_businesses_2020_retail_ageb,
    economic_growth_2020_retail_ageb,
    total_businesses_2023_retail_ageb,
    economic_growth_2023_retail_ageb,
    
    -- RETAIL - MUNICIPAL
    total_businesses_2010_retail_municipal,
    total_businesses_2015_retail_municipal,
    economic_growth_2015_retail_municipal,
    total_businesses_2017_retail_municipal,
    economic_growth_2017_retail_municipal,
    total_businesses_2020_retail_municipal,
    economic_growth_2020_retail_municipal,
    total_businesses_2023_retail_municipal,
    economic_growth_2023_retail_municipal,
    
    -- RETAIL - ENTIDAD
    total_businesses_2010_retail_entidad,
    total_businesses_2015_retail_entidad,
    economic_growth_2015_retail_entidad,
    total_businesses_2017_retail_entidad,
    economic_growth_2017_retail_entidad,
    total_businesses_2020_retail_entidad,
    economic_growth_2020_retail_entidad,
    total_businesses_2023_retail_entidad,
    economic_growth_2023_retail_entidad,
    
    -- ==============================================
    -- CATEGORY: PETS
    -- ==============================================
    
    -- PETS - AGEB
    total_businesses_2010_pets_ageb,
    total_businesses_2015_pets_ageb,
    economic_growth_2015_pets_ageb,
    total_businesses_2017_pets_ageb,
    economic_growth_2017_pets_ageb,
    total_businesses_2020_pets_ageb,
    economic_growth_2020_pets_ageb,
    total_businesses_2023_pets_ageb,
    economic_growth_2023_pets_ageb,
    
    -- PETS - MUNICIPAL
    total_businesses_2010_pets_municipal,
    total_businesses_2015_pets_municipal,
    economic_growth_2015_pets_municipal,
    total_businesses_2017_pets_municipal,
    economic_growth_2017_pets_municipal,
    total_businesses_2020_pets_municipal,
    economic_growth_2020_pets_municipal,
    total_businesses_2023_pets_municipal,
    economic_growth_2023_pets_municipal,
    
    -- PETS - ENTIDAD
    total_businesses_2010_pets_entidad,
    total_businesses_2015_pets_entidad,
    economic_growth_2015_pets_entidad,
    total_businesses_2017_pets_entidad,
    economic_growth_2017_pets_entidad,
    total_businesses_2020_pets_entidad,
    economic_growth_2020_pets_entidad,
    total_businesses_2023_pets_entidad,
    economic_growth_2023_pets_entidad,
    
    -- ==============================================
    -- CATEGORY: ATTRACTIONS_AND_ACTIVITIES
    -- ==============================================
    
    -- ATTRACTIONS_AND_ACTIVITIES - AGEB
    total_businesses_2010_attractions_and_activities_ageb,
    total_businesses_2015_attractions_and_activities_ageb,
    economic_growth_2015_attractions_and_activities_ageb,
    total_businesses_2017_attractions_and_activities_ageb,
    economic_growth_2017_attractions_and_activities_ageb,
    total_businesses_2020_attractions_and_activities_ageb,
    economic_growth_2020_attractions_and_activities_ageb,
    total_businesses_2023_attractions_and_activities_ageb,
    economic_growth_2023_attractions_and_activities_ageb,
    
    -- ATTRACTIONS_AND_ACTIVITIES - MUNICIPAL
    total_businesses_2010_attractions_and_activities_municipal,
    total_businesses_2015_attractions_and_activities_municipal,
    economic_growth_2015_attractions_and_activities_municipal,
    total_businesses_2017_attractions_and_activities_municipal,
    economic_growth_2017_attractions_and_activities_municipal,
    total_businesses_2020_attractions_and_activities_municipal,
    economic_growth_2020_attractions_and_activities_municipal,
    total_businesses_2023_attractions_and_activities_municipal,
    economic_growth_2023_attractions_and_activities_municipal,
    
    -- ATTRACTIONS_AND_ACTIVITIES - ENTIDAD
    total_businesses_2010_attractions_and_activities_entidad,
    total_businesses_2015_attractions_and_activities_entidad,
    economic_growth_2015_attractions_and_activities_entidad,
    total_businesses_2017_attractions_and_activities_entidad,
    economic_growth_2017_attractions_and_activities_entidad,
    total_businesses_2020_attractions_and_activities_entidad,
    economic_growth_2020_attractions_and_activities_entidad,
    total_businesses_2023_attractions_and_activities_entidad,
    economic_growth_2023_attractions_and_activities_entidad,
    
    -- ==============================================
    -- CATEGORY: EDUCATION
    -- ==============================================
    
    -- EDUCATION - AGEB
    total_businesses_2010_education_ageb,
    total_businesses_2015_education_ageb,
    economic_growth_2015_education_ageb,
    total_businesses_2017_education_ageb,
    economic_growth_2017_education_ageb,
    total_businesses_2020_education_ageb,
    economic_growth_2020_education_ageb,
    total_businesses_2023_education_ageb,
    economic_growth_2023_education_ageb,
    
    -- EDUCATION - MUNICIPAL
    total_businesses_2010_education_municipal,
    total_businesses_2015_education_municipal,
    economic_growth_2015_education_municipal,
    total_businesses_2017_education_municipal,
    economic_growth_2017_education_municipal,
    total_businesses_2020_education_municipal,
    economic_growth_2020_education_municipal,
    total_businesses_2023_education_municipal,
    economic_growth_2023_education_municipal,
    
    -- EDUCATION - ENTIDAD
    total_businesses_2010_education_entidad,
    total_businesses_2015_education_entidad,
    economic_growth_2015_education_entidad,
    total_businesses_2017_education_entidad,
    economic_growth_2017_education_entidad,
    total_businesses_2020_education_entidad,
    economic_growth_2020_education_entidad,
    total_businesses_2023_education_entidad,
    economic_growth_2023_education_entidad,
    
    -- ==============================================
    -- CATEGORY: OTHERS
    -- ==============================================
    
    -- OTHERS - AGEB
    total_businesses_2010_others_ageb,
    total_businesses_2015_others_ageb,
    economic_growth_2015_others_ageb,
    total_businesses_2017_others_ageb,
    economic_growth_2017_others_ageb,
    total_businesses_2020_others_ageb,
    economic_growth_2020_others_ageb,
    total_businesses_2023_others_ageb,
    economic_growth_2023_others_ageb,
    
    -- OTHERS - MUNICIPAL
    total_businesses_2010_others_municipal,
    total_businesses_2015_others_municipal,
    economic_growth_2015_others_municipal,
    total_businesses_2017_others_municipal,
    economic_growth_2017_others_municipal,
    total_businesses_2020_others_municipal,
    economic_growth_2020_others_municipal,
    total_businesses_2023_others_municipal,
    economic_growth_2023_others_municipal,
    
    -- OTHERS - ENTIDAD
    total_businesses_2010_others_entidad,
    total_businesses_2015_others_entidad,
    economic_growth_2015_others_entidad,
    total_businesses_2017_others_entidad,
    economic_growth_2017_others_entidad,
    total_businesses_2020_others_entidad,
    economic_growth_2020_others_entidad,
    total_businesses_2023_others_entidad,
    economic_growth_2023_others_entidad

FROM blackprint_db_prd.data_product.v_parcel_v3
where fid = {fid}
        '''
        return query
    
    @staticmethod
    def get_market_info_query(spot2, inmuebles24, propiedades):
        if inmuebles24 :
            query = f'''
                    SELECT
                    "id_market_data_inmuebles24" AS "id_market_data_inmuebles24",
                    "title" AS "title",
                    "description" AS "description",
                    "rent_price" AS "rent_price",
                    "rent_price_clean" AS "rent_price_clean",
                    "rent_price_per_m2" AS "rent_price_per_m2",
                    "buy_price" AS "buy_price",
                    "buy_price_clean" AS "buy_price_clean",
                    "buy_price_per_m2" AS "buy_price_per_m2",
                    "maintenance_price" AS "maintenance_price",
                    "publication_date" AS "publication_date",
                    "parking_lot" AS "parking_lot",
                    "bathrooms" AS "bathrooms",
                    "bedrooms" AS "bedrooms",
                    "age" AS "age",
                    "pictures" AS "pictures",
                    "property_type" AS "property_type",
                    "operation_type" AS "operation_type",
                    "property_dimension" AS "total_area",
                    "property_dimension_clean" AS "total_area_clean",
                    "zone" AS "zone",
                    "city" AS "city",
                    "address" AS "address",
                    "url" AS "url",
                    "amenities" AS "amenities"
                    from blackprint_db_prd.presentation.dim_market_data_inmuebles24
                    where id_market_data_inmuebles24 = {inmuebles24} '''
        elif spot2:
            query = f""" SELECT
                        "id_market_data_spot2" AS "id_market_data_spot2",
                        "title" AS "title",
                        "address" AS "address",
                        "street_address" AS "street_address",
                        "city" AS "city",
                        "zip_code" AS "zip_code",
                        "description" AS "description",
                        "operation_type" AS "operation_type",
                        "rent_price" AS "rent_price",
                        "rent_price_clean" AS "rent_price_clean",
                        "rent_price_per_m2" AS "rent_price_per_m2",
                        "buy_price" AS "buy_price",
                        "buy_price_clean" AS "buy_price_clean",
                        "buy_price_per_m2" AS "buy_price_per_m2",
                        "maintenance_price" AS "maintenance_price",
                        "property_type" AS "property_type",
                        "total_area" AS "total_area",
                        "total_area_clean" AS "total_area_clean",
                        "amenities" AS "amenities",
                        "pictures" AS "pictures",
                        "url" AS "url",
                        "parking_spaces" AS "parking_spaces",
                        "condition" AS "condition",
                        "date_published" AS "publication_date"
                    FROM
                    blackprint_db_prd.presentation.dim_market_data_spot2
                    WHERE id_market_data_spot2 = {spot2} """       
        elif propiedades :
            query = f""" 
                    SELECT
                    "id_market_data_propiedades" AS "id_market_data_propiedades",
                    "url" AS "url",
                    "property_type" AS "property_type",
                    "description" AS "description",
                    "buy_price" AS "buy_price",
                    "buy_price_usd" AS "buy_price_usd",
                    "buy_price_clean" AS "buy_price_clean",
                    "buy_price_per_m2" AS "buy_price_per_m2",
                    "rent_price" AS "rent_price",
                    "rent_price_usd" AS "rent_price_usd",
                    "rent_price_clean" AS "rent_price_clean",
                    "rent_price_per_m2" AS "rent_price_per_m2",
                    "size" AS "size",
                    "total_area_clean" AS "total_area_clean",
                    "postal_code" AS "postal_code",
                    "street_address" AS "street_address",
                    "bedrooms" AS "bedrooms",
                    "bathrooms" AS "bathrooms",
                    "geometry_coords" AS "geometry_coords"
                    FROM
                    blackprint_db_prd.presentation.dim_market_data_propiedades
                    where id_market_data_propiedades = {propiedades}
                """

        return query


