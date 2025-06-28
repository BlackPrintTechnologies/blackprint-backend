def get_commercial_growth_json(result):
    result = result[0]
    return {
        "commercial_growth": {
            "block": {
                "total_businesses_2010": result["total_businesses_2010_ageb"],
                "total_businesses_2015": result["total_businesses_2015_ageb"],
                "economic_growth_2015": result["economic_growth_2015_ageb"],
                "total_businesses_2017": result["total_businesses_2017_ageb"],
                "economic_growth_2017": result["economic_growth_2017_ageb"],
                "total_businesses_2020": result["total_businesses_2020_ageb"],
                "economic_growth_2020": result["economic_growth_2020_ageb"],
                "total_businesses_2023": result["total_businesses_2023_ageb"],
                "economic_growth_2023": result["economic_growth_2023_ageb"]
            },
            "alcaldia": {
                "total_businesses_2010": result["total_businesses_2010_municipal"],
                "total_businesses_2015": result["total_businesses_2015_municipal"],
                "economic_growth_2015": result["economic_growth_2015_municipal"],
                "total_businesses_2017": result["total_businesses_2017_municipal"],
                "economic_growth_2017": result["economic_growth_2017_municipal"],
                "total_businesses_2020": result["total_businesses_2020_municipal"],
                "economic_growth_2020": result["economic_growth_2020_municipal"],
                "total_businesses_2023": result["total_businesses_2023_municipal"],
                "economic_growth_2023": result["economic_growth_2023_municipal"]
            },
            "colonia": {
                "total_businesses_2010": result["total_businesses_2010_entidad"],
                "total_businesses_2015": result["total_businesses_2015_entidad"],
                "economic_growth_2015": result["economic_growth_2015_entidad"],
                "total_businesses_2017": result["total_businesses_2017_entidad"],
                "economic_growth_2017": result["economic_growth_2017_entidad"],
                "total_businesses_2020": result["total_businesses_2020_entidad"],
                "economic_growth_2020": result["economic_growth_2020_entidad"],
                "total_businesses_2023": result["total_businesses_2023_entidad"],
                "economic_growth_2023": result["economic_growth_2023_entidad"]
            }
        },
        "categories": {
            "EAT_AND_DRINK": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_eat_and_drink_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_eat_and_drink_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_eat_and_drink_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_eat_and_drink_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_eat_and_drink_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_eat_and_drink_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_eat_and_drink_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_eat_and_drink_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_eat_and_drink_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_eat_and_drink_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_eat_and_drink_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_eat_and_drink_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_eat_and_drink_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_eat_and_drink_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_eat_and_drink_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_eat_and_drink_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_eat_and_drink_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_eat_and_drink_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_eat_and_drink_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_eat_and_drink_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_eat_and_drink_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_eat_and_drink_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_eat_and_drink_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_eat_and_drink_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_eat_and_drink_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_eat_and_drink_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_eat_and_drink_entidad"]
                }
            },
            "HEALTH_AND_MEDICAL": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_health_and_medical_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_health_and_medical_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_health_and_medical_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_health_and_medical_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_health_and_medical_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_health_and_medical_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_health_and_medical_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_health_and_medical_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_health_and_medical_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_health_and_medical_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_health_and_medical_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_health_and_medical_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_health_and_medical_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_health_and_medical_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_health_and_medical_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_health_and_medical_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_health_and_medical_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_health_and_medical_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_health_and_medical_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_health_and_medical_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_health_and_medical_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_health_and_medical_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_health_and_medical_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_health_and_medical_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_health_and_medical_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_health_and_medical_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_health_and_medical_entidad"]
                }
            },
            "BEAUTY_AND_SPA": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_beauty_and_spa_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_beauty_and_spa_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_beauty_and_spa_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_beauty_and_spa_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_beauty_and_spa_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_beauty_and_spa_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_beauty_and_spa_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_beauty_and_spa_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_beauty_and_spa_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_beauty_and_spa_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_beauty_and_spa_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_beauty_and_spa_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_beauty_and_spa_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_beauty_and_spa_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_beauty_and_spa_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_beauty_and_spa_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_beauty_and_spa_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_beauty_and_spa_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_beauty_and_spa_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_beauty_and_spa_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_beauty_and_spa_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_beauty_and_spa_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_beauty_and_spa_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_beauty_and_spa_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_beauty_and_spa_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_beauty_and_spa_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_beauty_and_spa_entidad"]
                }
            },
            "FINANCIAL_SERVICE": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_financial_service_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_financial_service_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_financial_service_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_financial_service_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_financial_service_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_financial_service_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_financial_service_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_financial_service_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_financial_service_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_financial_service_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_financial_service_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_financial_service_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_financial_service_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_financial_service_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_financial_service_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_financial_service_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_financial_service_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_financial_service_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_financial_service_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_financial_service_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_financial_service_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_financial_service_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_financial_service_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_financial_service_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_financial_service_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_financial_service_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_financial_service_entidad"]
                }
            },
            "ARTS_AND_ENTERTAINMENT": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_arts_and_entertainment_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_arts_and_entertainment_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_arts_and_entertainment_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_arts_and_entertainment_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_arts_and_entertainment_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_arts_and_entertainment_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_arts_and_entertainment_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_arts_and_entertainment_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_arts_and_entertainment_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_arts_and_entertainment_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_arts_and_entertainment_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_arts_and_entertainment_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_arts_and_entertainment_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_arts_and_entertainment_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_arts_and_entertainment_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_arts_and_entertainment_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_arts_and_entertainment_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_arts_and_entertainment_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_arts_and_entertainment_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_arts_and_entertainment_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_arts_and_entertainment_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_arts_and_entertainment_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_arts_and_entertainment_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_arts_and_entertainment_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_arts_and_entertainment_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_arts_and_entertainment_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_arts_and_entertainment_entidad"]
                }
            },
            "ACTIVE_LIFE": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_active_life_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_active_life_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_active_life_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_active_life_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_active_life_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_active_life_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_active_life_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_active_life_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_active_life_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_active_life_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_active_life_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_active_life_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_active_life_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_active_life_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_active_life_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_active_life_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_active_life_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_active_life_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_active_life_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_active_life_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_active_life_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_active_life_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_active_life_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_active_life_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_active_life_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_active_life_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_active_life_entidad"]
                }
            },
            "RETAIL": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_retail_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_retail_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_retail_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_retail_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_retail_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_retail_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_retail_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_retail_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_retail_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_retail_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_retail_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_retail_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_retail_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_retail_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_retail_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_retail_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_retail_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_retail_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_retail_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_retail_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_retail_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_retail_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_retail_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_retail_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_retail_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_retail_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_retail_entidad"]
                }
            },
            "PETS": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_pets_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_pets_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_pets_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_pets_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_pets_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_pets_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_pets_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_pets_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_pets_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_pets_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_pets_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_pets_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_pets_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_pets_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_pets_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_pets_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_pets_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_pets_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_pets_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_pets_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_pets_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_pets_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_pets_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_pets_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_pets_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_pets_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_pets_entidad"]
                }
            },
            "ATTRACTIONS_AND_ACTIVITIES": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_attractions_and_activities_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_attractions_and_activities_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_attractions_and_activities_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_attractions_and_activities_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_attractions_and_activities_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_attractions_and_activities_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_attractions_and_activities_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_attractions_and_activities_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_attractions_and_activities_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_attractions_and_activities_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_attractions_and_activities_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_attractions_and_activities_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_attractions_and_activities_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_attractions_and_activities_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_attractions_and_activities_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_attractions_and_activities_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_attractions_and_activities_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_attractions_and_activities_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_attractions_and_activities_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_attractions_and_activities_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_attractions_and_activities_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_attractions_and_activities_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_attractions_and_activities_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_attractions_and_activities_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_attractions_and_activities_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_attractions_and_activities_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_attractions_and_activities_entidad"]
                }
            },
            "EDUCATION": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_education_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_education_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_education_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_education_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_education_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_education_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_education_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_education_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_education_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_education_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_education_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_education_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_education_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_education_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_education_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_education_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_education_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_education_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_education_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_education_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_education_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_education_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_education_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_education_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_education_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_education_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_education_entidad"]
                }
            },
            "OTHERS": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_others_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_others_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_others_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_others_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_others_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_others_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_others_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_others_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_others_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_others_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_others_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_others_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_others_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_others_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_others_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_others_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_others_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_others_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_others_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_others_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_others_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_others_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_others_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_others_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_others_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_others_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_others_entidad"]
                }
            }
        }
    } 