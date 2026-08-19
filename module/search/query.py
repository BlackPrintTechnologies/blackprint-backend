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
