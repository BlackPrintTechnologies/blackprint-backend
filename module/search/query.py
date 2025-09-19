class QueryController:
    def __init__(self, city='queretaro'):
        self.city = city
        self.table_ses_name = self._get_ses_table_name()
        self.parcel_table_name = self._get_parcel_table_name()
        self.table_mobility_name = self._get_mobility_table_name()
        self.table_pois_name = self._get_pois_table_name()
        pass

    def _get_ses_table_name(self):
        if self.city == 'queretaro' or self.city == 'el_marques':
            return 'blackprint_db_prd.presentation.dim_ses_ageb_qro'
        else:
            return 'blackprint_db_prd.presentation.dim_ses_ageb'

    def _get_parcel_table_name(self):
        if self.city == 'queretaro' or self.city == 'el_marques':
            return 'blackprint_db_prd.data_product.v_qro'
        else:
            return 'blackprint_db_prd.data_product.v_parcel_v3'
    
    def _get_mobility_table_name(self):
        if self.city == 'queretaro' or self.city == 'el_marques':
            return 'blackprint_db_prd.presentation.dataset_mobility_data_h3_qro'
        else:
            return 'blackprint_db_prd.presentation.dataset_mobility_data_h3'
    
    def _get_pois_table_name(self):
        if self.city == 'queretaro' or self.city == 'el_marques':
            return 'blackprint_db_prd.presentation.dim_pois_qro'
        else:
            return 'blackprint_db_prd.presentation.dim_pois_cdmx'


    def _get_socioeconomic_query(self, lat, lng, radius):
        
        query = f"""
            SELECT
            id_ses_ageb AS "id_ses_ageb",
            state_code AS "state_code",
            municipality_code AS "municipality_code",
            locality_code AS "locality_code",
            ageb_code AS "ageb_code",
            ses_ab AS "ses_ab",
            ses_c_plus AS "ses_c_plus",
            ses_c AS "ses_c",
            ses_c_minus AS "ses_c_minus",
            ses_d_plus AS "ses_d_plus",
            ses_d AS "ses_d",
            ses_e AS "ses_e",
            predominant_level AS "predominant_level",
            total_houses AS "total_houses",
            locality_size AS "locality_size"
            FROM
            {self.table_ses_name}
            INNER JOIN {self.parcel_table_name} ON {self.table_ses_name}.id_ses_ageb = {self.parcel_table_name}.id_ses_ageb
            WHERE 
            ST_Distance(
                ST_GeomFromText(
                    'POINT(' || 
                    CAST(JSON_EXTRACT(centroid, '$.coordinates[0]') AS VARCHAR) || ' ' || 
                    CAST(JSON_EXTRACT(centroid, '$.coordinates[1]') AS VARCHAR) || 
                    ')', 4326
                ),
                ST_GeomFromText('POINT({lng} {lat})', 4326)
            ) <= {radius}
        """
        return query
    
    def _get_mobility_query(self, lat, lng, radius):
        query = f"""
            WITH point_geom AS (
                SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
            ),
            point_projected AS (
                SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
            ),
            buffered AS (
                -- radius in meters
                SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
            ),
            h3_values AS (
                SELECT H3_Polyfill(ST_Transform(geom, 4326), 10) AS h3_indexes FROM buffered
            ),
            h3_index AS (
                SELECT 
                    o::VARCHAR AS h3_value,
                    LENGTH(o::VARCHAR) AS varchar_length,
                    o::BIGINT AS bigint_value
                FROM h3_values i, i.h3_indexes o
            )
            SELECT * 
            FROM {self.table_mobility_name} a
            INNER JOIN h3_index b ON a.h3_index = b.h3_value
        """
        return query

    def get_active_search_query(self, user_id):
        query = f"""
            SELECT * FROM active_search WHERE user_id = {user_id}
        """
        return query

    def _get_pois_query(self, lat, lng, radius):
        query = f"""
            WITH point_geom AS (
                SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
                ),
                point_projected AS (
                SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
                ),
                buffered AS (
                -- radius n meters
                SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
                ),
                h3_values AS (
                SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
                ),
                h3_index AS (
                    SELECT o AS h3_value
                    FROM h3_values i, i.h3_indexes o
                )
                SELECT case when chain_id != 'None' then chain_id else null end as brand, name as names_pri, geometry_wkt, main_category ,
                         sub_category , sub_sub_category, business_category, open_closed_status, popularity_score, average_stars, number_of_reviews, sentiment_score
                FROM {self.table_pois_name} a
                INNER JOIN h3_index b ON a.h3_value = b.h3_value
        """
    
        return query