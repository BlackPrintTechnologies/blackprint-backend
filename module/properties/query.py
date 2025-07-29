"""
Refactored QueryController using Unified City Management System
Eliminates over 1500 lines of redundant column definitions and hardcoded city logic
COMPLETE VERSION - All methods that controller actually calls
"""
from utils.city_manager import city_manager, query_builder


class QueryController:
    """
    Unified QueryController that eliminates redundancy between QRO and Mexico cities.
    Contains ONLY the methods actually called by the controller.
    """

    def __init__(self):
        self.city_manager = city_manager
        self.query_builder = query_builder

    @staticmethod
    def get_property_query(filter_clause, city='mexico', show_all_keys=True):
        """
        Main property query method - now clean and unified.
        Replaces 1500+ lines of duplicated column definitions.
        Called by controller: self.qc.get_property_query(filter_query, city=city)
        """
        normalized_city = city_manager.normalize_city_name(city)
        return query_builder.build_complete_property_query(filter_clause, normalized_city, show_all_keys)

    @staticmethod
    def get_demographics_query(fid, city='mexico'):
        """
        Demographics query using centralized city management.
        Called by controller: self.qc.get_demographics_query(fid, city=city)
        COMPLETE VERSION with ALL population growth columns restored.
        """
        normalized_city = city_manager.normalize_city_name(city)
        
        if city_manager.is_qro_city(normalized_city):
            # QRO demographic query - COMPLETE with all population growth columns
            query = f'''select 
                id_stg_demographic_socioeconomic_qro as fid,
                cvegep, cve_ent, nom_ent, cve_mun, nom_mun, cve_loc, nom_loc,
                cve_ageb, cve_mza, ambito, data_sourc, pobtot, pea, pea_m, pea_f,
                p_18ymas, p_15ymas, p_12ymas, p_3ymas, p_0a2, p_3a5, p_6a11,
                p_12a14, p_15a17, p_18a24, p_60ymas, pob15_64, pob65_mas,
                p15ym_an, p15ym_se, p15pri_in, p15pri_co, p15sec_in, p15sec_co,
                p18a24a, p18ym_pb, graproes, graproes_m, grapoes_f, p3a5_noa,
                p6a11_noa, p8a14an, p12a14noa, pe_inac, pe_inac_m, pe_inac_f,
                pocupada, pocupada_m, pocupada_f, pdesocup, pdesocup_m, pdesocup_f,
                niv_predom, tot_vivien, tot_viv_ab, tot_viv_cp, tot_viv_c, tot_viv_cm,
                tot_viv_dp, tot_viv_d, tot_viv_e, pct_viv_ab, pct_viv_cp,
                pct_viv_c, pct_viv_cm, pct_viv_dp, pct_viv_d, pct_viv_e,
                geometry_type, bbox,
                -- Population growth fields AGEB level
                pob_2000_ageb, pob_2005_ageb, pob_2010_ageb, pob_2015_ageb, pob_2020_ageb,
                cambio_porcentual_2005_ageb, cambio_porcentual_2010_ageb,
                cambio_porcentual_2015_ageb, cambio_porcentual_2020_ageb,
                -- Population growth fields Entity level
                pob_2000_entidad, pob_2005_entidad, pob_2010_entidad,
                pob_2015_entidad, pob_2020_entidad, cambio_porcentual_2005_entidad,
                cambio_porcentual_2010_entidad, cambio_porcentual_2015_entidad,
                cambio_porcentual_2020_entidad,
                -- Population growth fields Municipal level
                pob_2000_municipal, pob_2005_municipal, pob_2010_municipal,
                pob_2015_municipal, pob_2020_municipal, cambio_porcentual_2005_municipal,
                cambio_porcentual_2010_municipal, cambio_porcentual_2015_municipal,
                cambio_porcentual_2020_municipal
                from blackprint_db_prd.data_product.v_qro
                where id_stg_demographic_socioeconomic_qro = {fid}
                    '''
        else:
            # CDMX demographic query - COMPLETE with ALL population growth columns
            query = f'''select 
                fid, neighborhood, nom_mun, predominant_level, ageb_code,
                vivtot, vivtot_colonia, vivtot_alcaldia, prom_ocup, prom_ocup_colonia, 
                prom_ocup_alcaldia, pro_ocup_c, pro_ocup_c_colonia, pro_ocup_c_alcaldia, 
                ses_ab, ses_ab_colonia, ses_ab_alcaldia, ses_c_plus, ses_c_plus_colonia,
                ses_c_plus_alcaldia, ses_c, ses_c_colonia, ses_c_alcaldia,
                ses_c_minus, ses_c_minus_colonia, ses_c_minus_alcaldia, ses_d,
                ses_d_colonia, ses_d_alcaldia, ses_d_plus, ses_d_plus_colonia,
                ses_d_plus_alcaldia, ses_e, ses_e_colonia, ses_e_alcaldia,
                pobtot, pobtot_colonia, pobtot_alcaldia, pobmas, pobmas_colonia,
                pobmas_alcaldia, pobfem, pobfem_colonia, pobfem_alcaldia,
                p_3a5, p_3a5_colonia, p_3a5_alcaldia, p3a5_noa, p3a5_noa_colonia,
                p3a5_noa_alcaldia, p_6a11, p_6a11_colonia, p_6a11_alcaldia,
                p6a11_noa, p6a11_noa_colonia, p6a11_noa_alcaldia, p_12a14,
                p_12a14_colonia, p_12a14_alcaldia, p12a14noa, p12a14noa_colonia,
                p12a14noa_alcaldia, p_15a17, p_15a17_colonia, p_15a17_alcaldia,
                p15a17a, p15a17a_colonia, p15a17a_alcaldia, p_18a24,
                p_18a24_colonia, p_18a24_alcaldia, p18a24a, p18a24a_colonia,
                p18a24a_alcaldia, pea, pea_colonia, pea_alcaldia, pea_m,
                pea_m_colonia, pea_m_alcaldia, pea_f, pea_f_colonia, pea_f_alcaldia,
                pe_inac, pe_inac_colonia, pe_inac_alcaldia, pe_inac_m, pe_inac_m_colonia,
                pe_inac_m_alcaldia, pe_inac_f, pe_inac_f_colonia, pe_inac_f_alcaldia,
                pocupada, pocupada_colonia, pocupada_alcaldia, pocupada_m,
                pocupada_m_colonia, pocupada_m_alcaldia, pocupada_f,
                pocupada_f_colonia, pocupada_f_alcaldia, pdesocup, pdesocup_colonia,
                pdesocup_alcaldia, pdesocup_m, pdesocup_m_colonia, pdesocup_m_alcaldia,
                pdesocup_f, pdesocup_f_colonia, pdesocup_f_alcaldia,
                -- Population growth columns for AGEB level (CRITICAL - these were missing!)
                pob_2000_ageb, pob_2005_ageb, pob_2010_ageb, pob_2015_ageb, pob_2020_ageb,
                cambio_porcentual_2005_ageb, cambio_porcentual_2010_ageb,
                cambio_porcentual_2015_ageb, cambio_porcentual_2020_ageb,
                -- Population growth columns for Municipal level
                pob_2000_municipal, pob_2005_municipal, pob_2010_municipal,
                pob_2015_municipal, pob_2020_municipal, cambio_porcentual_2005_municipal,
                cambio_porcentual_2010_municipal, cambio_porcentual_2015_municipal,
                cambio_porcentual_2020_municipal,
                -- Population growth columns for Entity level
                pob_2000_entidad, pob_2005_entidad, pob_2010_entidad,
                pob_2015_entidad, pob_2020_entidad, cambio_porcentual_2005_entidad,
                cambio_porcentual_2010_entidad, cambio_porcentual_2015_entidad,
                cambio_porcentual_2020_entidad
                from blackprint_db_prd.data_product.v_parcel_v3
                where fid = {fid}
                '''
        return query

    @staticmethod
    def get_commercial_growth_query(fid):
        """
        Commercial growth query - Mexico City only (COMPLETE VERSION).
        Called by controller: self.qc.get_commercial_growth_query(fid)
        Includes ALL 11 categories that commercial_growth_json.py expects.
        """
        query = f'''SELECT 
            fid,
            -- AGEB
            total_businesses_2010_ageb, total_businesses_2015_ageb, economic_growth_2015_ageb,
            total_businesses_2017_ageb, economic_growth_2017_ageb, total_businesses_2020_ageb,
            economic_growth_2020_ageb, total_businesses_2023_ageb, economic_growth_2023_ageb,
            -- MUNICIPAL  
            total_businesses_2010_municipal, total_businesses_2015_municipal, economic_growth_2015_municipal,
            total_businesses_2017_municipal, economic_growth_2017_municipal, total_businesses_2020_municipal,
            economic_growth_2020_municipal, total_businesses_2023_municipal, economic_growth_2023_municipal,
            -- ENTIDAD
            total_businesses_2010_entidad, total_businesses_2015_entidad, economic_growth_2015_entidad,
            total_businesses_2017_entidad, economic_growth_2017_entidad, total_businesses_2020_entidad,
            economic_growth_2020_entidad, total_businesses_2023_entidad, economic_growth_2023_entidad,
            
            -- EAT_AND_DRINK
            total_businesses_2010_eat_and_drink_ageb, total_businesses_2015_eat_and_drink_ageb, economic_growth_2015_eat_and_drink_ageb,
            total_businesses_2017_eat_and_drink_ageb, economic_growth_2017_eat_and_drink_ageb, total_businesses_2020_eat_and_drink_ageb,
            economic_growth_2020_eat_and_drink_ageb, total_businesses_2023_eat_and_drink_ageb, economic_growth_2023_eat_and_drink_ageb,
            total_businesses_2010_eat_and_drink_municipal, total_businesses_2015_eat_and_drink_municipal, economic_growth_2015_eat_and_drink_municipal,
            total_businesses_2017_eat_and_drink_municipal, economic_growth_2017_eat_and_drink_municipal, total_businesses_2020_eat_and_drink_municipal,
            economic_growth_2020_eat_and_drink_municipal, total_businesses_2023_eat_and_drink_municipal, economic_growth_2023_eat_and_drink_municipal,
            total_businesses_2010_eat_and_drink_entidad, total_businesses_2015_eat_and_drink_entidad, economic_growth_2015_eat_and_drink_entidad,
            total_businesses_2017_eat_and_drink_entidad, economic_growth_2017_eat_and_drink_entidad, total_businesses_2020_eat_and_drink_entidad,
            economic_growth_2020_eat_and_drink_entidad, total_businesses_2023_eat_and_drink_entidad, economic_growth_2023_eat_and_drink_entidad,
            
            -- HEALTH_AND_MEDICAL
            total_businesses_2010_health_and_medical_ageb, total_businesses_2015_health_and_medical_ageb, economic_growth_2015_health_and_medical_ageb,
            total_businesses_2017_health_and_medical_ageb, economic_growth_2017_health_and_medical_ageb, total_businesses_2020_health_and_medical_ageb,
            economic_growth_2020_health_and_medical_ageb, total_businesses_2023_health_and_medical_ageb, economic_growth_2023_health_and_medical_ageb,
            total_businesses_2010_health_and_medical_municipal, total_businesses_2015_health_and_medical_municipal, economic_growth_2015_health_and_medical_municipal,
            total_businesses_2017_health_and_medical_municipal, economic_growth_2017_health_and_medical_municipal, total_businesses_2020_health_and_medical_municipal,
            economic_growth_2020_health_and_medical_municipal, total_businesses_2023_health_and_medical_municipal, economic_growth_2023_health_and_medical_municipal,
            total_businesses_2010_health_and_medical_entidad, total_businesses_2015_health_and_medical_entidad, economic_growth_2015_health_and_medical_entidad,
            total_businesses_2017_health_and_medical_entidad, economic_growth_2017_health_and_medical_entidad, total_businesses_2020_health_and_medical_entidad,
            economic_growth_2020_health_and_medical_entidad, total_businesses_2023_health_and_medical_entidad, economic_growth_2023_health_and_medical_entidad,
            
            -- BEAUTY_AND_SPA
            total_businesses_2010_beauty_and_spa_ageb, total_businesses_2015_beauty_and_spa_ageb, economic_growth_2015_beauty_and_spa_ageb,
            total_businesses_2017_beauty_and_spa_ageb, economic_growth_2017_beauty_and_spa_ageb, total_businesses_2020_beauty_and_spa_ageb,
            economic_growth_2020_beauty_and_spa_ageb, total_businesses_2023_beauty_and_spa_ageb, economic_growth_2023_beauty_and_spa_ageb,
            total_businesses_2010_beauty_and_spa_municipal, total_businesses_2015_beauty_and_spa_municipal, economic_growth_2015_beauty_and_spa_municipal,
            total_businesses_2017_beauty_and_spa_municipal, economic_growth_2017_beauty_and_spa_municipal, total_businesses_2020_beauty_and_spa_municipal,
            economic_growth_2020_beauty_and_spa_municipal, total_businesses_2023_beauty_and_spa_municipal, economic_growth_2023_beauty_and_spa_municipal,
            total_businesses_2010_beauty_and_spa_entidad, total_businesses_2015_beauty_and_spa_entidad, economic_growth_2015_beauty_and_spa_entidad,
            total_businesses_2017_beauty_and_spa_entidad, economic_growth_2017_beauty_and_spa_entidad, total_businesses_2020_beauty_and_spa_entidad,
            economic_growth_2020_beauty_and_spa_entidad, total_businesses_2023_beauty_and_spa_entidad, economic_growth_2023_beauty_and_spa_entidad,
            
            -- FINANCIAL_SERVICE
            total_businesses_2010_financial_service_ageb, total_businesses_2015_financial_service_ageb, economic_growth_2015_financial_service_ageb,
            total_businesses_2017_financial_service_ageb, economic_growth_2017_financial_service_ageb, total_businesses_2020_financial_service_ageb,
            economic_growth_2020_financial_service_ageb, total_businesses_2023_financial_service_ageb, economic_growth_2023_financial_service_ageb,
            total_businesses_2010_financial_service_municipal, total_businesses_2015_financial_service_municipal, economic_growth_2015_financial_service_municipal,
            total_businesses_2017_financial_service_municipal, economic_growth_2017_financial_service_municipal, total_businesses_2020_financial_service_municipal,
            economic_growth_2020_financial_service_municipal, total_businesses_2023_financial_service_municipal, economic_growth_2023_financial_service_municipal,
            total_businesses_2010_financial_service_entidad, total_businesses_2015_financial_service_entidad, economic_growth_2015_financial_service_entidad,
            total_businesses_2017_financial_service_entidad, economic_growth_2017_financial_service_entidad, total_businesses_2020_financial_service_entidad,
            economic_growth_2020_financial_service_entidad, total_businesses_2023_financial_service_entidad, economic_growth_2023_financial_service_entidad,
            
            -- ARTS_AND_ENTERTAINMENT
            total_businesses_2010_arts_and_entertainment_ageb, total_businesses_2015_arts_and_entertainment_ageb, economic_growth_2015_arts_and_entertainment_ageb,
            total_businesses_2017_arts_and_entertainment_ageb, economic_growth_2017_arts_and_entertainment_ageb, total_businesses_2020_arts_and_entertainment_ageb,
            economic_growth_2020_arts_and_entertainment_ageb, total_businesses_2023_arts_and_entertainment_ageb, economic_growth_2023_arts_and_entertainment_ageb,
            total_businesses_2010_arts_and_entertainment_municipal, total_businesses_2015_arts_and_entertainment_municipal, economic_growth_2015_arts_and_entertainment_municipal,
            total_businesses_2017_arts_and_entertainment_municipal, economic_growth_2017_arts_and_entertainment_municipal, total_businesses_2020_arts_and_entertainment_municipal,
            economic_growth_2020_arts_and_entertainment_municipal, total_businesses_2023_arts_and_entertainment_municipal, economic_growth_2023_arts_and_entertainment_municipal,
            total_businesses_2010_arts_and_entertainment_entidad, total_businesses_2015_arts_and_entertainment_entidad, economic_growth_2015_arts_and_entertainment_entidad,
            total_businesses_2017_arts_and_entertainment_entidad, economic_growth_2017_arts_and_entertainment_entidad, total_businesses_2020_arts_and_entertainment_entidad,
            economic_growth_2020_arts_and_entertainment_entidad, total_businesses_2023_arts_and_entertainment_entidad, economic_growth_2023_arts_and_entertainment_entidad,
            
            -- ACTIVE_LIFE
            total_businesses_2010_active_life_ageb, total_businesses_2015_active_life_ageb, economic_growth_2015_active_life_ageb,
            total_businesses_2017_active_life_ageb, economic_growth_2017_active_life_ageb, total_businesses_2020_active_life_ageb,
            economic_growth_2020_active_life_ageb, total_businesses_2023_active_life_ageb, economic_growth_2023_active_life_ageb,
            total_businesses_2010_active_life_municipal, total_businesses_2015_active_life_municipal, economic_growth_2015_active_life_municipal,
            total_businesses_2017_active_life_municipal, economic_growth_2017_active_life_municipal, total_businesses_2020_active_life_municipal,
            economic_growth_2020_active_life_municipal, total_businesses_2023_active_life_municipal, economic_growth_2023_active_life_municipal,
            total_businesses_2010_active_life_entidad, total_businesses_2015_active_life_entidad, economic_growth_2015_active_life_entidad,
            total_businesses_2017_active_life_entidad, economic_growth_2017_active_life_entidad, total_businesses_2020_active_life_entidad,
            economic_growth_2020_active_life_entidad, total_businesses_2023_active_life_entidad, economic_growth_2023_active_life_entidad,
            
            -- RETAIL
            total_businesses_2010_retail_ageb, total_businesses_2015_retail_ageb, economic_growth_2015_retail_ageb,
            total_businesses_2017_retail_ageb, economic_growth_2017_retail_ageb, total_businesses_2020_retail_ageb,
            economic_growth_2020_retail_ageb, total_businesses_2023_retail_ageb, economic_growth_2023_retail_ageb,
            total_businesses_2010_retail_municipal, total_businesses_2015_retail_municipal, economic_growth_2015_retail_municipal,
            total_businesses_2017_retail_municipal, economic_growth_2017_retail_municipal, total_businesses_2020_retail_municipal,
            economic_growth_2020_retail_municipal, total_businesses_2023_retail_municipal, economic_growth_2023_retail_municipal,
            total_businesses_2010_retail_entidad, total_businesses_2015_retail_entidad, economic_growth_2015_retail_entidad,
            total_businesses_2017_retail_entidad, economic_growth_2017_retail_entidad, total_businesses_2020_retail_entidad,
            economic_growth_2020_retail_entidad, total_businesses_2023_retail_entidad, economic_growth_2023_retail_entidad,
            
            -- PETS
            total_businesses_2010_pets_ageb, total_businesses_2015_pets_ageb, economic_growth_2015_pets_ageb,
            total_businesses_2017_pets_ageb, economic_growth_2017_pets_ageb, total_businesses_2020_pets_ageb,
            economic_growth_2020_pets_ageb, total_businesses_2023_pets_ageb, economic_growth_2023_pets_ageb,
            total_businesses_2010_pets_municipal, total_businesses_2015_pets_municipal, economic_growth_2015_pets_municipal,
            total_businesses_2017_pets_municipal, economic_growth_2017_pets_municipal, total_businesses_2020_pets_municipal,
            economic_growth_2020_pets_municipal, total_businesses_2023_pets_municipal, economic_growth_2023_pets_municipal,
            total_businesses_2010_pets_entidad, total_businesses_2015_pets_entidad, economic_growth_2015_pets_entidad,
            total_businesses_2017_pets_entidad, economic_growth_2017_pets_entidad, total_businesses_2020_pets_entidad,
            economic_growth_2020_pets_entidad, total_businesses_2023_pets_entidad, economic_growth_2023_pets_entidad,
            
            -- ATTRACTIONS_AND_ACTIVITIES
            total_businesses_2010_attractions_and_activities_ageb, total_businesses_2015_attractions_and_activities_ageb, economic_growth_2015_attractions_and_activities_ageb,
            total_businesses_2017_attractions_and_activities_ageb, economic_growth_2017_attractions_and_activities_ageb, total_businesses_2020_attractions_and_activities_ageb,
            economic_growth_2020_attractions_and_activities_ageb, total_businesses_2023_attractions_and_activities_ageb, economic_growth_2023_attractions_and_activities_ageb,
            total_businesses_2010_attractions_and_activities_municipal, total_businesses_2015_attractions_and_activities_municipal, economic_growth_2015_attractions_and_activities_municipal,
            total_businesses_2017_attractions_and_activities_municipal, economic_growth_2017_attractions_and_activities_municipal, total_businesses_2020_attractions_and_activities_municipal,
            economic_growth_2020_attractions_and_activities_municipal, total_businesses_2023_attractions_and_activities_municipal, economic_growth_2023_attractions_and_activities_municipal,
            total_businesses_2010_attractions_and_activities_entidad, total_businesses_2015_attractions_and_activities_entidad, economic_growth_2015_attractions_and_activities_entidad,
            total_businesses_2017_attractions_and_activities_entidad, economic_growth_2017_attractions_and_activities_entidad, total_businesses_2020_attractions_and_activities_entidad,
            economic_growth_2020_attractions_and_activities_entidad, total_businesses_2023_attractions_and_activities_entidad, economic_growth_2023_attractions_and_activities_entidad,
            
            -- EDUCATION
            total_businesses_2010_education_ageb, total_businesses_2015_education_ageb, economic_growth_2015_education_ageb,
            total_businesses_2017_education_ageb, economic_growth_2017_education_ageb, total_businesses_2020_education_ageb,
            economic_growth_2020_education_ageb, total_businesses_2023_education_ageb, economic_growth_2023_education_ageb,
            total_businesses_2010_education_municipal, total_businesses_2015_education_municipal, economic_growth_2015_education_municipal,
            total_businesses_2017_education_municipal, economic_growth_2017_education_municipal, total_businesses_2020_education_municipal,
            economic_growth_2020_education_municipal, total_businesses_2023_education_municipal, economic_growth_2023_education_municipal,
            total_businesses_2010_education_entidad, total_businesses_2015_education_entidad, economic_growth_2015_education_entidad,
            total_businesses_2017_education_entidad, economic_growth_2017_education_entidad, total_businesses_2020_education_entidad,
            economic_growth_2020_education_entidad, total_businesses_2023_education_entidad, economic_growth_2023_education_entidad,
            
            -- OTHERS
            total_businesses_2010_others_ageb, total_businesses_2015_others_ageb, economic_growth_2015_others_ageb,
            total_businesses_2017_others_ageb, economic_growth_2017_others_ageb, total_businesses_2020_others_ageb,
            economic_growth_2020_others_ageb, total_businesses_2023_others_ageb, economic_growth_2023_others_ageb,
            total_businesses_2010_others_municipal, total_businesses_2015_others_municipal, economic_growth_2015_others_municipal,
            total_businesses_2017_others_municipal, economic_growth_2017_others_municipal, total_businesses_2020_others_municipal,
            economic_growth_2020_others_municipal, total_businesses_2023_others_municipal, economic_growth_2023_others_municipal,
            total_businesses_2010_others_entidad, total_businesses_2015_others_entidad, economic_growth_2015_others_entidad,
            total_businesses_2017_others_entidad, economic_growth_2017_others_entidad, total_businesses_2020_others_entidad,
            economic_growth_2020_others_entidad, total_businesses_2023_others_entidad, economic_growth_2023_others_entidad
            
            FROM blackprint_db_prd.data_product.v_parcel_v3
            where fid = {fid}
            '''
        return query

    @staticmethod  
    def get_market_info_query(spot2, inmuebles24, propiedades, city='mexico'):
        """
        Market info query using centralized city management.
        Called by controller: self.qc.get_market_info_query(spot2_id, inmuebles24_id, propiedades_id, city=city)
        """
        normalized_city = city_manager.normalize_city_name(city)
        
        if inmuebles24:
            if city_manager.is_qro_city(normalized_city):
                query = f'''SELECT
                        "id_market_data_inmuebles24" AS "id_market_data_inmuebles24",
                        "title" AS "title", "description" AS "description",
                        "rent_price" AS "rent_price", "rent_price_clean" AS "rent_price_clean",
                        "rent_price_per_m2" AS "rent_price_per_m2", "buy_price" AS "buy_price",
                        "buy_price_clean" AS "buy_price_clean", "buy_price_per_m2" AS "buy_price_per_m2",
                        "maintenance_price" AS "maintenance_price", "publication_date" AS "publication_date",
                        "parking_lot" AS "parking_lot", "bathrooms" AS "bathrooms",
                        "bedrooms" AS "bedrooms", "age" AS "age", "pictures" AS "pictures",
                        "property_type" AS "property_type", "operation_type" AS "operation_type",
                        "property_dimension" AS "total_area", "property_dimension_clean" AS "total_area_clean",
                        "zone" AS "zone", "city" AS "city", "address" AS "address",
                        "url" AS "url", "amenities" AS "amenities"
                        FROM blackprint_db_prd.presentation.dim_market_data_inmuebles24_qro
                        WHERE id_market_data_inmuebles24 = {inmuebles24} '''
            else:
                query = f'''SELECT
                        "id_market_data_inmuebles24" AS "id_market_data_inmuebles24",
                        "title" AS "title", "description" AS "description",
                        "rent_price" AS "rent_price", "rent_price_clean" AS "rent_price_clean",
                        "rent_price_per_m2" AS "rent_price_per_m2", "buy_price" AS "buy_price",
                        "buy_price_clean" AS "buy_price_clean", "buy_price_per_m2" AS "buy_price_per_m2",
                        "maintenance_price" AS "maintenance_price", "publication_date" AS "publication_date",
                        "parking_lot" AS "parking_lot", "bathrooms" AS "bathrooms",
                        "bedrooms" AS "bedrooms", "age" AS "age", "pictures" AS "pictures",
                        "property_type" AS "property_type", "operation_type" AS "operation_type",
                        "property_dimension" AS "total_area", "property_dimension_clean" AS "total_area_clean",
                        "zone" AS "zone", "city" AS "city", "address" AS "address",
                        "url" AS "url", "amenities" AS "amenities"
                        FROM blackprint_db_prd.presentation.dim_market_data_inmuebles24
                        WHERE id_market_data_inmuebles24 = {inmuebles24} '''
        elif spot2:
            if city_manager.is_qro_city(normalized_city):
                query = f"""SELECT
                            "id_market_data_spot2" AS "id_market_data_spot2",
                            "title" AS "title", "address" AS "address", "street_address" AS "street_address",
                            "city" AS "city", "zip_code" AS "zip_code", "description" AS "description",
                            "operation_type" AS "operation_type", "rent_price" AS "rent_price",
                            "rent_price_clean" AS "rent_price_clean", "rent_price_per_m2" AS "rent_price_per_m2",
                            "buy_price" AS "buy_price", "buy_price_clean" AS "buy_price_clean",
                            "buy_price_per_m2" AS "buy_price_per_m2", "maintenance_price" AS "maintenance_price",
                            "property_type" AS "property_type", "total_area" AS "total_area",
                            "total_area_clean" AS "total_area_clean", "amenities" AS "amenities",
                            "pictures" AS "pictures", "url" AS "url", "parking_spaces" AS "parking_spaces",
                            "condition" AS "condition", "date_published" AS "publication_date"
                        FROM blackprint_db_prd.presentation.dim_market_data_spot2_qro
                        WHERE id_market_data_spot2 = {spot2} """
            else:
                query = f"""SELECT
                            "id_market_data_spot2" AS "id_market_data_spot2",
                            "title" AS "title", "address" AS "address", "street_address" AS "street_address",
                            "city" AS "city", "zip_code" AS "zip_code", "description" AS "description",
                            "operation_type" AS "operation_type", "rent_price" AS "rent_price",
                            "rent_price_clean" AS "rent_price_clean", "rent_price_per_m2" AS "rent_price_per_m2",
                            "buy_price" AS "buy_price", "buy_price_clean" AS "buy_price_clean",
                            "buy_price_per_m2" AS "buy_price_per_m2", "maintenance_price" AS "maintenance_price",
                            "property_type" AS "property_type", "total_area" AS "total_area",
                            "total_area_clean" AS "total_area_clean", "amenities" AS "amenities",
                            "pictures" AS "pictures", "url" AS "url", "parking_spaces" AS "parking_spaces",
                            "condition" AS "condition", "date_published" AS "publication_date"
                        FROM blackprint_db_prd.presentation.dim_market_data_spot2
                        WHERE id_market_data_spot2 = {spot2} """       
        elif propiedades:
            if city_manager.is_qro_city(normalized_city):
                # QRO propiedades - table doesn't exist, return empty result
                query = f"""SELECT
                        NULL AS "id_market_data_propiedades", NULL AS "url", NULL AS "property_type",
                        NULL AS "description", NULL AS "buy_price", NULL AS "buy_price_usd",
                        NULL AS "buy_price_clean", NULL AS "buy_price_per_m2", NULL AS "rent_price",
                        NULL AS "rent_price_usd", NULL AS "rent_price_clean", NULL AS "rent_price_per_m2",
                        NULL AS "size", NULL AS "total_area_clean", NULL AS "postal_code",
                        NULL AS "street_address", NULL AS "bedrooms", NULL AS "bathrooms",
                        NULL AS "geometry_coords"
                        WHERE 1=0 """
            else:
                query = f"""SELECT
                        "id_market_data_propiedades" AS "id_market_data_propiedades",
                        "url" AS "url", "property_type" AS "property_type", "description" AS "description",
                        "buy_price" AS "buy_price", "buy_price_usd" AS "buy_price_usd",
                        "buy_price_clean" AS "buy_price_clean", "buy_price_per_m2" AS "buy_price_per_m2",
                        "rent_price" AS "rent_price", "rent_price_usd" AS "rent_price_usd",
                        "rent_price_clean" AS "rent_price_clean", "rent_price_per_m2" AS "rent_price_per_m2",
                        "size" AS "size", "total_area_clean" AS "total_area_clean",
                        "postal_code" AS "postal_code", "street_address" AS "street_address",
                        "bedrooms" AS "bedrooms", "bathrooms" AS "bathrooms",
                        "geometry_coords" AS "geometry_coords"
                        FROM blackprint_db_prd.presentation.dim_market_data_propiedades
                        WHERE id_market_data_propiedades = {propiedades} """
        else:
            query = "SELECT NULL WHERE 1=0"
        
        return query