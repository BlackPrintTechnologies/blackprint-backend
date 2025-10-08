from utils.dbUtils import Database, RedshiftDatabase
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from datetime import datetime
from decimal import Decimal
import logging
from module.area_analysis.query import AreaAnalysisQuery
#for adding brand icon links in the brand api
from utils.iconUtils import IconMapper

logger = logging.getLogger(__name__)

class ActiveSearchController:
    def __init__(self):
        logger.debug("Initializing ActiveSearchController")
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.qc = AreaAnalysisQuery()
    
    def get_active_search(self, user_id):
        """Get active search data for a user."""
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Getting active search data for user: {user_id}")
            
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.qc.get_active_search_query(user_id)
            logger.info(f"Active search query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            logger.info(f"Active search results count: {len(res)}")
            
            # Process the results to ensure proper data types
            processed_results = []
            for row in res:
                row_dict = dict(row)
                # Convert any Decimal fields to float for JSON serialization
                for key, value in row_dict.items():
                    if isinstance(value, Decimal):
                        row_dict[key] = float(value)
                processed_results.append(row_dict)
            
            resp = Response.success(data={"response": processed_results})
            
        except Exception as e:
            logger.error(f"Error in get_active_search: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp
    
    def get_mobility_data(self, lat, lng, radius, city='queretaro'):
        """Get mobility data within specified radius from lat/lng coordinates."""
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Getting mobility data - lat: {lat}, lng: {lng}, radius: {radius}, city: {city}")
            
            query = self.qc._get_mobility_query(lat, lng, radius)
                        
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            logger.info(f"Mobility query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            logger.info(f"Mobility results count: {len(res)}")
            
            # Process the results to ensure proper data types
            processed_results = []
            for row in res:
                row_dict = dict(row)
                # Convert any Decimal fields to float for JSON serialization
                for key, value in row_dict.items():
                    if isinstance(value, Decimal):
                        row_dict[key] = float(value)
                processed_results.append(row_dict)
            
            resp = Response.success(data={"response": processed_results})
            
        except Exception as e:
            logger.error(f"Error in get_mobility_data: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp
    
    def get_socioeconomic_data(self, lat, lng, radius, city='queretaro'):
        """Get socioeconomic data within specified radius from lat/lng coordinates."""
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Getting socioeconomic data - lat: {lat}, lng: {lng}, radius: {radius}, city: {city}")
            
            query = self.qc._get_socioeconomic_query(lat, lng, radius)

        except Exception as e:
            logger.error(f"Error in get_socioeconomic_data: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp 
    
    def get_pois_data(self, lat, lng, radius, city='queretaro'):
        """Get POIs data within specified radius from lat/lng coordinates with comprehensive area analysis."""
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Getting POIs data - lat: {lat}, lng: {lng}, radius: {radius}, city: {city}")
            
            query = self.qc._get_pois_query(lat, lng, radius)
            population_query = self.qc._get_total_population_query(lat, lng, radius, city)
            
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            logger.info(f"POIs query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            #execute the population query
            cursor.execute(population_query)
            connection.commit()
            population_res = cursor.fetchall()
            print("population_res", population_res)
            
            
            logger.info(f"POIs results count: {len(res)}")
            
            # Process the results to ensure proper data types
            processed_results = []
            for row in res:
                row_dict = dict(row)
                # Convert any Decimal fields to float for JSON serialization
                for key, value in row_dict.items():
                    if isinstance(value, Decimal):
                        row_dict[key] = float(value)
                processed_results.append(row_dict)
                
            #get the total population of the selected area
            total_population  = 0
            if population_res and len(population_res) > 0:
                total_population = population_res[0]["total_population"]
            else:
                total_population = 0
            
            
            # Calculate comprehensive area analysis metrics
            analysis_metrics = self._calculate_pois_analysis_metrics(processed_results,total_population)
            
            resp = Response.success(data={
                "analysis_metrics": analysis_metrics
            })
            
        except Exception as e:
            logger.error(f"Error in get_pois_data: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp
    
    def _calculate_pois_analysis_metrics(self, pois_data,total_population=0):
        """Calculate comprehensive area analysis metrics from POIs data."""
        try:
            if not pois_data:
                return {
                    "total_pois": 0,
                    "message": "No POIs found in the specified area",
                    "bussiness_density": 0
                }
            
            # Initialize metrics
            metrics = {
                "total_pois": len(pois_data),
                "brand_analysis": {},
                "category_analysis": {},
                "review_analysis": {},
                "business_insights": {},
                "top_categories": [],
                "top_brands": [],
                "quality_metrics": {}
            }
            
            # Brand Analysis
            brand_counts = {}
            brand_reviews = {}
            brand_ratings = {}
            brand_popularity = {}
            
            # Category Analysis
            main_category_counts = {}
            sub_category_counts = {}
            business_category_counts = {}
            
            # Review Analysis
            total_reviews = 0
            total_rating = 0
            rating_count = 0
            review_distribution = {"0-10": 0, "11-50": 0, "51-100": 0, "100+": 0}
            
            # Quality Metrics
            open_pois = 0
            closed_pois = 0
            high_popularity_pois = 0
            high_rating_pois = 0
            
            # Process each POI
            for poi in pois_data:
                # Brand Analysis
                brand = poi.get('brand')
                if brand and brand != 'None':
                    brand_counts[brand] = brand_counts.get(brand, 0) + 1
                    
                    # Brand reviews and ratings
                    reviews = poi.get('num_reviews', 0) or 0
                    rating = poi.get('average_stars', 0) or 0
                    
                    if brand not in brand_reviews:
                        brand_reviews[brand] = 0
                        brand_ratings[brand] = []
                    
                    brand_reviews[brand] += reviews
                    if rating > 0:
                        brand_ratings[brand].append(rating)
                    
                    # Brand popularity
                    popularity = poi.get('popularity_score', 0) or 0
                    if brand not in brand_popularity:
                        brand_popularity[brand] = []
                    brand_popularity[brand].append(popularity)
                
                # Category Analysis
                main_cat = poi.get('main_category', 'Unknown')
                sub_cat = poi.get('sub_category', 'Unknown')
                business_cat = poi.get('business_category', 'Unknown')
                
                main_category_counts[main_cat] = main_category_counts.get(main_cat, 0) + 1
                sub_category_counts[sub_cat] = sub_category_counts.get(sub_cat, 0) + 1
                business_category_counts[business_cat] = business_category_counts.get(business_cat, 0) + 1
                
                # Review Analysis
                reviews = poi.get('num_reviews', 0) or 0
                rating = poi.get('average_stars', 0) or 0
                
                total_reviews += reviews
                if rating > 0:
                    total_rating += rating
                    rating_count += 1
                
                # Review distribution
                if reviews <= 10:
                    review_distribution["0-10"] += 1
                elif reviews <= 50:
                    review_distribution["11-50"] += 1
                elif reviews <= 100:
                    review_distribution["51-100"] += 1
                else:
                    review_distribution["100+"] += 1
                
                # Quality Metrics
                status = poi.get('open_closed_status', 'Unknown')
                if status == 'Open':
                    open_pois += 1
                elif status == 'Closed':
                    closed_pois += 1
                
                popularity = poi.get('popularity_score', 0) or 0
                if popularity > 0.7:  # Assuming popularity score is 0-1
                    high_popularity_pois += 1
                
                if rating >= 3:
                    high_rating_pois += 1
            
            # Calculate brand metrics
            for brand in brand_counts:
                avg_rating = sum(brand_ratings[brand]) / len(brand_ratings[brand]) if brand_ratings[brand] else 0
                avg_popularity = sum(brand_popularity[brand]) / len(brand_popularity[brand]) if brand_popularity[brand] else 0
                
                metrics["brand_analysis"][brand] = {
                    "count": brand_counts[brand],
                    "total_reviews": brand_reviews[brand],
                    "average_rating": round(avg_rating, 2),
                    "average_popularity": round(avg_popularity, 2),
                    
                }
            
            # Top 10 Brands by count
            metrics["top_brands"] = [
                {
                    "brand": brand,
                    "count":data["count"],
                    "icon_url": IconMapper.get_brand_url(brand)
                }
                for brand , data in metrics["brand_analysis"].items()
            ]
            metrics["top_brands"] = sorted(metrics["top_brands"], key=lambda x: x["count"], reverse=True)[:10]
            # Top 10 Brands by reviews
            metrics["top_brands_by_reviews"] = sorted(
                [(brand, data["total_reviews"]) for brand, data in metrics["brand_analysis"].items()],
                key=lambda x: x[1], reverse=True
            )[:10]
            
            # Category Analysis
            metrics["category_analysis"] = {
                "main_categories": dict(sorted(main_category_counts.items(), key=lambda x: x[1], reverse=True)),
                "sub_categories": dict(sorted(sub_category_counts.items(), key=lambda x: x[1], reverse=True)),
                "business_categories": dict(sorted(business_category_counts.items(), key=lambda x: x[1], reverse=True))
            }
            
            # Top 10 Categories
            metrics["top_categories"] = sorted(
                main_category_counts.items(), 
                key=lambda x: x[1], reverse=True
            )[:10]
            
            # Review Analysis
            avg_rating = total_rating / rating_count if rating_count > 0 else 0
            metrics["review_analysis"] = {
                "total_reviews": total_reviews,
                "average_rating": round(avg_rating, 2),
                "pois_with_ratings": rating_count,
                "review_distribution": review_distribution,
                "most_reviewed_categories": sorted(
                    [(cat, sum(poi.get('num_reviews', 0) or 0 for poi in pois_data if poi.get('main_category') == cat)) 
                     for cat in main_category_counts.keys()],
                    key=lambda x: x[1], reverse=True
                )[:10]
            }
            
            # Business Insights
            metrics["business_insights"] = {
                "open_pois": open_pois,
                "closed_pois": closed_pois,
                "open_rate": round((open_pois / len(pois_data)) * 100, 2) if pois_data else 0,
                "high_popularity_pois": high_popularity_pois,
                "high_rating_pois": high_rating_pois,
                "diversity_score": len(main_category_counts),  # Number of unique categories
                "brand_diversity": len(brand_counts)
            }
            
            # Quality Metrics
            metrics["quality_metrics"] = {
                "average_popularity": round(
                    sum(poi.get('popularity_score', 0) or 0 for poi in pois_data) / len(pois_data), 2
                ) if pois_data else 0,
                "high_quality_pois": high_rating_pois + high_popularity_pois,
                "quality_score": round(
                    ((high_rating_pois + high_popularity_pois) / len(pois_data)) * 100, 2
                ) if pois_data else 0
            }
            #calculation for the chains and independent pois
            chain_count = 0
            independent_count = 0
            for poi in pois_data:
                brand = poi.get('brand')
                if brand and brand != 'None' and brand.strip() != '':
                    chain_count += 1
                else:
                    independent_count += 1
            #calcultate %s
            chain_percentage = round((chain_count / len(pois_data)) * 100, 2)
            independent_percentage = round((independent_count / len(pois_data)) * 100, 2)
            
            #add to the metrics
            metrics["chain_analysis"] = {
                "chains":{
                    "count": chain_count,
                    "percentage": chain_percentage
                },
                "independent":{
                    "count": independent_count,
                    "percentage": independent_percentage
                },
                "total_pois": len(pois_data)
                }
            
            #calculating the bussiness density rate per 1000 people
            bussiness_density_rate = 0
            if total_population > 0:
                bussiness_density_rate = round((len(pois_data) / total_population) * 1000, 2)
            else:
                bussiness_density_rate = 0
            #main categories with count and percentage
            main_categories_detailed = {}
            for category, count in main_category_counts.items():
                percentage = round((count / len(pois_data)) * 100, 2)
                main_categories_detailed[category] = {
                    "count": count,
                    "percentage": percentage
                }
            # Additional insights
            metrics["insights"] = {
                "most_common_category": max(main_category_counts.items(), key=lambda x: x[1]) if main_category_counts else ("None", 0),
                "most_reviewed_brand": max(
                    [(brand, data["total_reviews"]) for brand, data in metrics["brand_analysis"].items()],
                    key=lambda x: x[1]
                ) if metrics["brand_analysis"] else ("None", 0),
                "highest_rated_brand": max(
                    [(brand, data["average_rating"]) for brand, data in metrics["brand_analysis"].items() if data["average_rating"] > 0],
                    key=lambda x: x[1]
                ) if metrics["brand_analysis"] else ("None", 0),
                "area_density": round(len(pois_data) / 1000, 2),  # POIs per 1000m² (assuming radius is in meters)
                "bussiness_density_rate": bussiness_density_rate, # for bussiness density per 1000 people,
                "main_categories_detailed": main_categories_detailed #main categories with count and percentage
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating POIs analysis metrics: {str(e)}", exc_info=True)
            return {
                "error": f"Failed to calculate analysis metrics: {str(e)}",
                "total_pois": len(pois_data) if pois_data else 0
            }
