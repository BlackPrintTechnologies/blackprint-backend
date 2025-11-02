"""Constants for area analysis module."""

DEMOGRAPHICS_DATA_FORMAT = {
    'summary': {
        'area_km2': 0.0,
        'population': 0.0,
        'population_density': 0.0,
        'population_density_formatted': '0.0 persons / km²',
        'center_point': {
            'lat': 0.0,
            'lng': 0.0
        },
        'radius_meters': 0
    },
    'demographics': {
        'total_population': 0.0,
        'male_population': 0.0,
        'female_population': 0.0,
        'male_percentage': 0.0,
        'female_percentage': 0.0,
        'total_households': 0.0,
        'average_household_size': 0.0
    },
    'detailed_data': {
        'general': {
            'block': {
                'neighborhood': '',
                'predominant_level': '',
                'ageb_code': '',
                'total_household': 0.0,
                'average_household_size': 0.0,
                'average_number_of_rooms': None
            },
            'colonia': {
                'neighborhood': '',
                'predominant_level': '',
                'ageb_code': '',
                'total_household': 0.0,
                'average_household_size': 0.0,
                'average_number_of_rooms': None
            },
            'alcaldia': {
                'neighborhood': '',
                'predominant_level': '',
                'ageb_code': '',
                'total_household': 0.0,
                'average_household_size': 0.0,
                'average_number_of_rooms': None
            }
        },
        'socio_economic_level': {
            'block': {
                'ses_ab': 0.0,
                'ses_c_plus': 0.0,
                'ses_c': 0.0,
                'ses_c_minus': 0.0,
                'ses_d': 0.0,
                'ses_d_plus': 0.0,
                'ses_e': 0.0
            },
            'colonia': {
                'ses_ab': 0.0,
                'ses_c_plus': 0.0,
                'ses_c': 0.0,
                'ses_c_minus': 0.0,
                'ses_d': 0.0,
                'ses_d_plus': 0.0,
                'ses_e': 0.0
            },
            'alcaldia': {
                'ses_ab': 0.0,
                'ses_c_plus': 0.0,
                'ses_c': 0.0,
                'ses_c_minus': 0.0,
                'ses_d': 0.0,
                'ses_d_plus': 0.0,
                'ses_e': 0.0
            }
        },
        'population': {
            'block': {
                'total_population': 0.0,
                'male_population': 0.0,
                'female_population': 0.0
            },
            'colonia': {
                'total_population': 0.0,
                'male_population': 0.0,
                'female_population': 0.0
            },
            'alcaldia': {
                'total_population': 0.0,
                'male_population': 0.0,
                'female_population': 0.0
            }
        },
        'education': {
            'block': {
                'education_3_5': 0.0,
                'education_6_11': 0.0,
                'education_12_14': 0.0,
                'education_15_17': 0.0,
                'education_18_24': 0.0,
                'education_3_5_attending_school': 0.0,
                'education_6_11_attending_school': 0.0,
                'education_12_14_attending_school': 0.0,
                'education_15_17_attending_school': 0.0,
                'education_18_24_attending_school': 0.0
            },
            'colonia': {
                'education_3_5': 0.0,
                'education_6_11': 0.0,
                'education_12_14': 0.0,
                'education_15_17': 0.0,
                'education_18_24': 0.0,
                'education_3_5_attending_school': 0.0,
                'education_6_11_attending_school': 0.0,
                'education_12_14_attending_school': 0.0,
                'education_15_17_attending_school': 0.0,
                'education_18_24_attending_school': 0.0
            },
            'alcaldia': {
                'education_3_5': 0.0,
                'education_6_11': 0.0,
                'education_12_14': 0.0,
                'education_15_17': 0.0,
                'education_18_24': 0.0,
                'education_3_5_attending_school': 0.0,
                'education_6_11_attending_school': 0.0,
                'education_12_14_attending_school': 0.0,
                'education_15_17_attending_school': 0.0,
                'education_18_24_attending_school': 0.0
            }
        },
        'workforce': {
            'block': {
                'total_workforce': 0.0,
                'total_male_workforce': 0.0,
                'total_female_workforce': 0.0,
                'total_inactive_population': 0.0,
                'total_inactive_male_population': 0.0,
                'total_inactive_female_population': 0.0
            },
            'colonia': {
                'total_workforce': 0.0,
                'total_male_workforce': 0.0,
                'total_female_workforce': 0.0,
                'total_inactive_population': 0.0,
                'total_inactive_male_population': 0.0,
                'total_inactive_female_population': 0.0
            },
            'alcaldia': {
                'total_workforce': 0.0,
                'total_male_workforce': 0.0,
                'total_female_workforce': 0.0,
                'total_inactive_population': 0.0,
                'total_inactive_male_population': 0.0,
                'total_inactive_female_population': 0.0
            }
        },
        'employment': {
            'block': {
                'total_employed_population': 0.0,
                'total_male_employed_population': 0.0,
                'total_female_emloyed_population': 0.0,
                'total_unemployed_population': 0.0,
                'total_unemployed_male_population': 0.0,
                'total_unemployed_female_population': 0.0
            },
            'colonia': {
                'total_employed_population': 0.0,
                'total_male_employed_population': 0.0,
                'total_female_emloyed_population': 0.0,
                'total_unemployed_population': 0.0,
                'total_unemployed_male_population': 0.0,
                'total_unemployed_female_population': 0.0
            },
            'alcaldia': {
                'total_employed_population': 0.0,
                'total_male_employed_population': 0.0,
                'total_female_emloyed_population': 0.0,
                'total_unemployed_population': 0.0,
                'total_unemployed_male_population': 0.0,
                'total_unemployed_female_population': 0.0
            }
        },
        'population_growth': {
            'block': {
                '2000': [None, None],
                '2005': [None, None],
                '2010': [None, None],
                '2015': [None, None],
                '2020': [None, None]
            },
            'colonia': {
                '2000': [None, None],
                '2005': [None, None],
                '2010': [None, None],
                '2015': [None, None],
                '2020': [None, None]
            },
            'alcaldia': {
                '2000': [None, None],
                '2005': [None, None],
                '2010': [None, None],
                '2015': [None, None],
                '2020': [None, None]
            }
        }
    },
    'age_pyramid_2024': {
        'age_groups': [],
        'total_population': 0.0
    },
    'population_growth_2024': {
        'area': [],
        'municipality': [],
        'years': [2000, 2005, 2010, 2020]
    },
    'comparison': {
        'selected_area': {
            'population_density': 0.0,
            'population_density_trend': '',
            'male_population': 0.0,
            'male_percentage': '0.0%',
            'male_trend': '',
            'female_population': 0.0,
            'female_percentage': '0.0%',
            'female_trend': '',
            'total_households': 0.0,
            'households_trend': ''
        },
        'municipality': {
            'population_density': 0.0,
            'male_population': 0.0,
            'male_percentage': '0.0%',
            'female_population': 0.0,
            'female_percentage': '0.0%',
            'total_households': 0.0
        }
    },
    'socioeconomic_analysis': {
        'predominant_socioeconomic_level': {
            'selected_area': {
                'level': '',
                'percentage': 0.0
            },
            'municipality': {
                'level': '',
                'percentage': 0.0
            }
        },
        'households_per_level': []
    },
    'socioeconomic_income_analysis': {
        'income_summary': {
            'total_households': 0,
            'total_household_income': 0,
            'average_household_income': 0.0
        },
        'income_distribution': {
            'levels': []
        },
        'historical_trends': {
            'growth_by_level': []
        },
        'municipality_analysis': {
            'income_summary': {
                'total_households': 0,
                'total_household_income': 0,
                'average_household_income': 0.0
            },
            'income_distribution': {
                'levels': []
            },
            'historical_trends': {
                'growth_by_level': []
            }
        }
    }
}

TRAFFIC_DATA_FORMAT = {
    'summary': {
        'num_parcels': None,
        'population': 0.0,
        'area_km2': 0.0,
        'center_point': {
            'lat': 0.0,
            'lng': 0.0
        },
        'radius_meters': 0
    },
    'socioeconomic': {
        'total_unique_devices': 0,
        'devices_per_person': 0.0,
        'municipality_average': 0.0
    },
    'traffic': {
        'vehicles': {
            'count': 0,
            'percentage': 0.0,
            'municipality_percentage': 0.0,
            'trend': None
        },
        'pedestrians': {
            'count': 0,
            'percentage': 0.0,
            'municipality_percentage': 0.0,
            'trend': None
        },
        'stationary_devices': {
            'count': 0,
            'percentage': 0.0,
            'municipality_percentage': 0.0,
            'trend': None
        }
    },
    'h3_traffic_summary': {
        'unique_h3_count': 0,
        'total_unique_users': 0,
        'avg_users_per_h3': 0.0,
        'percentile_rank': 0.0
    },
    'h3_distribution': {
        'total_h3_indexes': 0,
        'bucket_distribution': {
            'data_range': {
                'min_value': 0,
                'max_value': 0
            },
            'buckets': [],
            'distribution_stats': {
                'total_points': 0,
                'bucket_count': 0,
                'mean_count': 0.0
            }
        }
    }
}
