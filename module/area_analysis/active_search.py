from module.area_analysis.formatter import format_pois_data
from utils.dbUtils import Database, RedshiftDatabase
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from utils.app_cache import set_in_cache, get_from_cache
from datetime import datetime
from decimal import Decimal
import logging
from module.area_analysis.query import AreaAnalysisQuery
#for adding brand icon links in the brand api
from utils.iconUtils import IconMapper
from module.area_data.controller import DemographicsAreaData, SocioeconomicAreaData, PoisAreaData, TotalPopulation

logger = logging.getLogger(__name__)

class ActiveSearchController:
    def __init__(self):
        logger.debug("Initializing ActiveSearchController")
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.qc = AreaAnalysisQuery()
    
    def get_active_search(self, user_id):
        """Get active search data for a user."""
        # Create cache key based on user_id
        cache_key = f"active_search_{user_id}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached active search data for user {user_id}")
            return cached_response
        
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
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached active search data for user {user_id}")
            
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
        # Create cache key based on parameters
        cache_key = f"mobility_data_{city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached mobility data for {city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
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
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached mobility data for {city} at ({lat}, {lng}) with radius {radius}")
            
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
        """Get POIs data with both area and municipality analysis."""
        # Create cache key based on parameters
        cache_key = f"pois_data_{city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached POIs data for {city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Getting POIs data - lat: {lat}, lng: {lng}, radius: {radius}, city: {city}")
            
            # Get area-based POI data
            area_query = self.qc._get_pois_query(lat, lng, radius)
            area_population_query = self.qc._get_total_population_query(lat, lng, radius, city)
            
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Execute area queries
            cursor.execute(area_query)
            connection.commit()
            area_res = cursor.fetchall()
            
            cursor.execute(area_population_query)
            connection.commit()
            area_population_res = cursor.fetchall()
            
            # Process area results
            area_processed_results = []
            for row in area_res:
                row_dict = dict(row)
                for key, value in row_dict.items():
                    if isinstance(value, Decimal):
                        row_dict[key] = float(value)
                area_processed_results.append(row_dict)
            
            area_population = area_population_res[0]["total_population"] if area_population_res and len(area_population_res) > 0 else 0
            
            # Calculate area analysis metrics
            area_analysis = self._calculate_pois_analysis_metrics(area_processed_results, area_population)
            
            # Handle case where area analysis fails
            if "error" in area_analysis:
                area_analysis = {
                    "total_pois": len(area_processed_results),
                    "error": area_analysis["error"]
                }
            
            # Get municipality data
            municipality_data = self._get_municipality_analysis(lat, lng, city, cursor)
            
            resp = Response.success(data={
                "analysis_metrics": area_analysis,
                "analysis_metrics_municipality": municipality_data
            })
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached POIs data for {city} at ({lat}, {lng}) with radius {radius}")
            
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
    
    def _get_municipality_analysis(self, lat, lng, city, cursor):
        """Get municipality analysis with business density calculation using direct H3_Polyfill."""
        try:
            # Step 1: Get municipality info from coordinates
            print("getting municipality info", lat, lng, city)
            municipality_query = self.qc._get_municipality_info_query(lat, lng, city)
            print("municipality_query", municipality_query)
            cursor.execute(municipality_query)
            municipality_res = cursor.fetchall()
            
            if not municipality_res or len(municipality_res) == 0:
                return {
                    "error": "No municipality found for the given coordinates",
                    "business_density_rate": 0
                }
            
            municipality_info = dict(municipality_res[0])
            municipality_code = municipality_info['municipality_code']
            municipality_name = municipality_info['municipality_name']
            municipality_population = municipality_info['municipality_population']
            
            logger.info(f"Found municipality: {municipality_name} (code: {municipality_code})")
            
            # Step 2: Get municipality POI data using direct H3_Polyfill
            print("getting municipality POIs using direct H3_Polyfill")
            municipality_pois_query = self.qc._get_municipality_pois_query_direct(lat, lng, city)
            print("municipality_pois_query", municipality_pois_query)
            
            cursor.execute(municipality_pois_query)
            municipality_pois_res = cursor.fetchall()
            
            # Process municipality POI results
            municipality_processed_results = []
            for row in municipality_pois_res:
                row_dict = dict(row)
                for key, value in row_dict.items():
                    if isinstance(value, Decimal):
                        row_dict[key] = float(value)
                municipality_processed_results.append(row_dict)
            
            # Calculate business density rate (POIs per 1000 population)
            business_density_rate = 0
            if municipality_population and municipality_population > 0:
                business_density_rate = round((len(municipality_processed_results) / municipality_population) * 1000, 2)
            
            # Calculate municipality analysis metrics
            municipality_analysis = self._calculate_pois_analysis_metrics(municipality_processed_results, municipality_population)
            
            # Handle case where analysis fails
            if "error" in municipality_analysis:
                return {
                    "total_pois": len(municipality_processed_results),
                    "municipality_code": municipality_code,
                    "municipality_name": municipality_name,
                    "municipality_population": municipality_population,
                    "bussiness_density_rate": business_density_rate,
                    "error": municipality_analysis["error"]
                }
            
            # Add municipality context to the analysis metrics
            municipality_analysis['municipality_code'] = municipality_code
            municipality_analysis['municipality_name'] = municipality_name
            municipality_analysis['municipality_population'] = municipality_population
            municipality_analysis['bussiness_density_rate'] = business_density_rate
            
            return municipality_analysis
            
        except Exception as e:
            logger.error(f"Error in _get_municipality_analysis: {str(e)}", exc_info=True)
            return {
                "error": f"Municipality analysis failed: {str(e)}",
                "business_density_rate": 0
            }
    
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
            brand_categories = {}  # Track categories for each brand
            
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
                    
                    # Track brand categories (use most common category for each brand)
                    main_cat = poi.get('main_category', 'Unknown')
                    if brand not in brand_categories:
                        brand_categories[brand] = {}
                    brand_categories[brand][main_cat] = brand_categories[brand].get(main_cat, 0) + 1
                    
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
            
            # Calculate total brand count for percentage calculation
            total_brand_count = sum(brand_counts.values())
            
            # Top 10 Brands by count with percentage and category
            metrics["top_brands"] = []
            for brand, data in metrics["brand_analysis"].items():
                # Get the most common category for this brand
                primary_category = "Unknown"
                if brand in brand_categories:
                    primary_category = max(brand_categories[brand].items(), key=lambda x: x[1])[0]
                
                metrics["top_brands"].append({
                    "brand": brand,
                    "count": data["count"],
                    "percentage": round((data["count"] / total_brand_count) * 100, 2) if total_brand_count > 0 else 0,
                    "category": primary_category,
                    "icon_url": IconMapper.get_brand_url(brand)
                })
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
            chain_percentage = round((chain_count / len(pois_data)) * 100, 2) if pois_data else 0
            independent_percentage = round((independent_count / len(pois_data)) * 100, 2) if pois_data else 0
            
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
            # Main categories with detailed breakdown including business and brands
            main_categories_detailed = []
            for category, count in main_category_counts.items():
                percentage = round((count / len(pois_data)) * 100, 2) if pois_data else 0
                
                # Get business categories for this main category
                business_in_category = [poi for poi in pois_data if poi.get('main_category') == category]
                business_category_counts_for_main = {}
                for poi in business_in_category:
                    business_cat = poi.get('business_category', 'Unknown')
                    business_category_counts_for_main[business_cat] = business_category_counts_for_main.get(business_cat, 0) + 1
                
                # Get brands for this main category
                brand_counts_for_category = {}
                for poi in business_in_category:
                    brand = poi.get('brand')
                    if brand and brand != 'None':
                        brand_counts_for_category[brand] = brand_counts_for_category.get(brand, 0) + 1
                
                # Calculate business percentages
                business_list = []
                for business_name, business_count in business_category_counts_for_main.items():
                    business_percentage = round((business_count / count) * 100, 2) if count > 0 else 0
                    business_list.append({
                        "name": business_name,
                        "count": business_count,
                        "percentage": business_percentage
                    })
                
                # Calculate brand percentages
                brands_list = []
                total_brand_count = sum(brand_counts_for_category.values())
                for brand_name, brand_count in brand_counts_for_category.items():
                    brand_percentage = round((brand_count / total_brand_count) * 100, 2) if total_brand_count > 0 else 0
                    
                    # Get the most common category for this brand
                    primary_category = "Unknown"
                    if brand_name in brand_categories:
                        primary_category = max(brand_categories[brand_name].items(), key=lambda x: x[1])[0]
                    
                    brands_list.append({
                        "name": brand_name,
                        "count": brand_count,
                        "percentage": brand_percentage,
                        "category": primary_category,
                        "icon_url": IconMapper.get_brand_url(brand_name)
                    })
                
                # Sort business and brands by count (descending)
                business_list.sort(key=lambda x: x["count"], reverse=True)
                brands_list.sort(key=lambda x: x["count"], reverse=True)
                
                # Limit brands to top 10 only
                brands_list = brands_list[:10]
                
                # Calculate total business and brand counts (sum of all counts)
                business_count = sum(business_category_counts_for_main.values())
                brand_count = sum(brand_counts_for_category.values())
                
                main_categories_detailed.append({
                    "category": category,
                    "count": count,
                    "percentage": percentage,
                    "business_count": business_count,
                    "brand_count": brand_count,
                    "business": business_list,
                    "brands": brands_list
                })

            # Sort main categories by count (descending)
            main_categories_detailed.sort(key=lambda x: x["count"], reverse=True)
            # Additional insights
            # Safely get most common category
            most_common_category = ("None", 0)
            if main_category_counts:
                most_common_category = max(main_category_counts.items(), key=lambda x: x[1])
            
            # Safely get most reviewed brand
            most_reviewed_brand = ("None", 0)
            if metrics["brand_analysis"]:
                reviewed_brands = [(brand, data["total_reviews"]) for brand, data in metrics["brand_analysis"].items() if data["total_reviews"] > 0]
                if reviewed_brands:
                    most_reviewed_brand = max(reviewed_brands, key=lambda x: x[1])
            
            # Safely get highest rated brand
            highest_rated_brand = ("None", 0)
            if metrics["brand_analysis"]:
                rated_brands = [(brand, data["average_rating"]) for brand, data in metrics["brand_analysis"].items() if data["average_rating"] > 0]
                if rated_brands:
                    highest_rated_brand = max(rated_brands, key=lambda x: x[1])
            
            metrics["insights"] = {
                "most_common_category": most_common_category,
                "most_reviewed_brand": most_reviewed_brand,
                "highest_rated_brand": highest_rated_brand,
                "area_density": round(len(pois_data) / 1000, 2),  # POIs per 1000m² (assuming radius is in meters)
                "bussiness_density_rate": bussiness_density_rate, # for bussiness density per 1000 people,
                "main_categories_detailed": main_categories_detailed # Updated format with business and brands
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating POIs analysis metrics: {str(e)}", exc_info=True)
            return {
                "error": f"Failed to calculate analysis metrics: {str(e)}",
                "total_pois": len(pois_data) if pois_data else 0
            }


    #get poi hierarchy with details
    def get_pois_hierarchy_with_details(self, lat, lng, radius, city='queretaro'):
        """Get POI category hierarchy with brand statistics within specified area."""
        # Create cache key based on parameters
        cache_key = f"pois_hierarchy_{city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached POI hierarchy data for {city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Getting POI hierarchy with brands - lat: {lat}, lng: {lng}, radius: {radius}, city: {city}")
            
            query = self.qc.get_pois_hierarchy_with_brands_query(lat, lng, radius, city)
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            logger.info(f"POI hierarchy with brands query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Build hierarchy structure with brand data
            hierarchy = self._build_hierarchy_with_brands_from_results(res)
            
            logger.info(f"POI hierarchy with brands built with {len(hierarchy)} top-level categories")
            resp = Response.success(data={"hierarchy": hierarchy})
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached POI hierarchy data for {city} at ({lat}, {lng}) with radius {radius}")
            
        except Exception as e:
            logger.error(f"Error in get_pois_hierarchy_with_brands: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def _build_hierarchy_with_brands_from_results(self, results):
        """Build hierarchical structure with brand statistics from database results."""
        hierarchy = {}
        
        for row in results:
            cat1 = row.get('category_1', '').strip()
            cat2 = row.get('category_2', '').strip()
            cat3 = row.get('category_3', '').strip()
            unique_brands = row.get('unique_brands', 0)
            total_pois = row.get('total_pois', 0)
            # Parse brand_list as JSON objects
            brand_list = []
            brand_list_json = row.get('brand_list', '')
            
            if brand_list_json:
                import json
                import re
                
                # Use regex to find complete JSON objects (handles commas within quoted strings)
                json_pattern = r'\{(?:[^{}]|"[^"]*")*\}'
                json_matches = re.findall(json_pattern, brand_list_json)
                
                for json_str in json_matches:
                    parsed_obj = json.loads(json_str)
                    brand_list.append(parsed_obj)
            
            if not cat1:
                continue
                
            # Create category_1 entry if it doesn't exist
            if cat1 not in hierarchy:
                hierarchy[cat1] = {
                    'id': cat1.lower().replace(' ', '_').replace('&', 'and'),
                    'name': cat1,
                    'brand_stats': {
                        'unique_brands': 0,
                        'total_pois': 0,
                        'brand_list': []
                    },
                    'subCategories': {}
                }
            
            # Update category_1 brand stats
            hierarchy[cat1]['brand_stats']['unique_brands'] += unique_brands
            hierarchy[cat1]['brand_stats']['total_pois'] += total_pois
            hierarchy[cat1]['brand_stats']['brand_list'].extend(brand_list)
            
            # Add category_2 if it exists
            if cat2 and cat2 != cat1:
                if cat2 not in hierarchy[cat1]['subCategories']:
                    hierarchy[cat1]['subCategories'][cat2] = {
                        'id': cat2.lower().replace(' ', '_').replace('&', 'and'),
                        'name': cat2,
                        'brand_stats': {
                            'unique_brands': 0,
                            'total_pois': 0,
                            'brand_list': []
                        },
                        'subSubCategories': {}
                    }
                
                # Update category_2 brand stats
                hierarchy[cat1]['subCategories'][cat2]['brand_stats']['unique_brands'] += unique_brands
                hierarchy[cat1]['subCategories'][cat2]['brand_stats']['total_pois'] += total_pois
                hierarchy[cat1]['subCategories'][cat2]['brand_stats']['brand_list'].extend(brand_list)
                
                # Add category_3 if it exists
                if cat3 and cat3 != cat2 and cat3 != cat1:
                    if cat3 not in hierarchy[cat1]['subCategories'][cat2]['subSubCategories']:
                        hierarchy[cat1]['subCategories'][cat2]['subSubCategories'][cat3] = {
                            'id': cat3.lower().replace(' ', '_').replace('&', 'and'),
                            'name': cat3,
                            'brand_stats': {
                                'unique_brands': unique_brands,
                                'total_pois': total_pois,
                                'brand_list': brand_list
                            }
                        }
        
        # Convert dictionaries to lists and clean up brand lists
        final_hierarchy = []
        for cat1_data in hierarchy.values():
            # Remove duplicates from brand lists based on brand name
            seen_brands = set()
            unique_brands = []
            for brand in cat1_data['brand_stats']['brand_list']:
                brand_name = brand.get('name', '') if isinstance(brand, dict) else brand
                if brand_name not in seen_brands:
                    seen_brands.add(brand_name)
                    unique_brands.append(brand)
            cat1_data['brand_stats']['brand_list'] = unique_brands
            
            cat1_entry = {
                'id': cat1_data['id'],
                'name': cat1_data['name'],
                'brand_stats': cat1_data['brand_stats'],
                'subCategories': []
            }
            
            for cat2_data in cat1_data['subCategories'].values():
                # Remove duplicates from brand lists based on brand name
                seen_brands = set()
                unique_brands = []
                for brand in cat2_data['brand_stats']['brand_list']:
                    brand_name = brand.get('name', '') if isinstance(brand, dict) else brand
                    if brand_name not in seen_brands:
                        seen_brands.add(brand_name)
                        unique_brands.append(brand)
                cat2_data['brand_stats']['brand_list'] = unique_brands
                
                cat2_entry = {
                    'id': cat2_data['id'],
                    'name': cat2_data['name'],
                    'brand_stats': cat2_data['brand_stats'],
                    'subSubCategories': []
                }
                
                for cat3_data in cat2_data['subSubCategories'].values():
                    # Remove duplicates from brand lists based on brand name
                    seen_brands = set()
                    unique_brands = []
                    for brand in cat3_data['brand_stats']['brand_list']:
                        brand_name = brand.get('name', '') if isinstance(brand, dict) else brand
                        if brand_name not in seen_brands:
                            seen_brands.add(brand_name)
                            unique_brands.append(brand)
                    cat3_data['brand_stats']['brand_list'] = unique_brands
                    
                    cat2_entry['subSubCategories'].append({
                        'id': cat3_data['id'],
                        'name': cat3_data['name'],
                        'brand_stats': cat3_data['brand_stats']
                    })
                
                cat1_entry['subCategories'].append(cat2_entry)
            
            final_hierarchy.append(cat1_entry)
        
        return final_hierarchy
    
        
############### NEW AREA ANALYSIS IMPLEMENTATION ###############

class AbstractActiveSearchController:
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.redshift_connection = self.redshift_db.connect()
        self.cursor = self.redshift_connection.cursor(cursor_factory=RealDictCursor)
        self.qc = AreaAnalysisQuery()
    

    def get_boundary_from_coordinates(self, lat, lng, radius, city='queretaro'):
        """Get boundary from coordinates as WKT polygon (radius in meters)."""
        # Build buffer around point in meters using Web Mercator, then return as WKT in 4326
        query = self.qc.get_boundary_from_coordinates_query(lat, lng, radius, city)
        self.cursor.execute(query)
        res = self.cursor.fetchone()
        return res.get('wkt') if res else None
    
    def get_municipality_info(self, lat, lng, city='queretaro'):
        """Get municipality info (code, name, population, boundary WKT) from coordinates."""
        query = self.qc.get_municipality_info_query(lat, lng, city)
        self.cursor.execute(query)
        res = self.cursor.fetchone()
        return res.get('wkt') if res else None


class NewActiveAreaPoisDataController(AbstractActiveSearchController):
    """Class for new active area POIs data operations."""

    DATA_FORMAT = {
        'analysis_metrics' : {
            'total_pois' : 0,
            'brand_analysis' : {},
            'category_analysis' : {},
            'review_analysis' : {},
            'business_insights' : {},
            'top_categories' : [],
            'top_brands' : [],
            'quality_metrics' : {},
            'chain_analysis' : {},
            'insights' : {
                'most_common_category' : ('None', 0),
                'most_reviewed_brand' : ('None', 0),
                'highest_rated_brand' : ('None', 0),
                'area_density' : 0,
                'bussiness_density_rate' : 0,
                'main_categories_detailed' : []
            }   
        },
        'analysis_metrics_municipality' : {
            'total_pois' : 0,
            'brand_analysis' : {},
            'category_analysis' : {},
            'review_analysis' : {},
            'business_insights' : {},
            'top_categories' : [],
            'top_brands' : [],
            'quality_metrics' : {},
            'chain_analysis' : {},
            'insights' : {
                'most_common_category' : ('None', 0),
                'most_reviewed_brand' : ('None', 0),
                'highest_rated_brand' : ('None', 0),
                'area_density' : 0,
                'bussiness_density_rate' : 0,
                'main_categories_detailed' : []
            }
        }
        }
    
    def __init__(self):
        """Initialize the NewActiveAreaPoisDataController class."""
        super().__init__()
    
    def get_data(self, lat, lng, radius, city='queretaro'):
        """Get active area POIs data for the selected area."""
        catchment = self.get_boundary_from_coordinates(lat, lng, radius, city)
        municipality_wkt = self.get_municipality_info(lat, lng, city)
        # Fetch POIs within catchment and municipality
        area_data = PoisAreaData().get_data(catchment) if catchment else []
        municipality_area_data = PoisAreaData().get_data(municipality_wkt) if municipality_wkt else []

        # Convert Decimals and normalize fields (e.g., number_of_reviews -> num_reviews)
        def prepare_rows(rows):
            prepared = []
            for row in rows or []:
                row_dict = dict(row)
                for key, value in list(row_dict.items()):
                    if isinstance(value, Decimal):
                        row_dict[key] = float(value)
                if 'number_of_reviews' in row_dict and 'num_reviews' not in row_dict:
                    row_dict['num_reviews'] = row_dict.get('number_of_reviews') or 0
                prepared.append(row_dict)
            return prepared

        area_rows = prepare_rows(area_data)
        municipality_rows = prepare_rows(municipality_area_data)

        # Query total population for area catchment (to compute business density rate)
        total_population_object = TotalPopulation()
        total_population = total_population_object.get_data(catchment)
        # Compute analysis metrics for area and municipality
        area_analysis = format_pois_data(area_rows, total_population)

        municipality_population = total_population_object.get_data(municipality_wkt)
        municipality_code = None
        municipality_name = None

        municipality_analysis = self._calculate_pois_analysis_metrics(municipality_rows, municipality_population)

        # Attach municipality context similar to _get_municipality_analysis
        if isinstance(municipality_analysis, dict):
            municipality_analysis['municipality_code'] = municipality_code
            municipality_analysis['municipality_name'] = municipality_name
            municipality_analysis['municipality_population'] = municipality_population
            bussiness_density_rate = 0
            if municipality_population and municipality_population > 0:
                bussiness_density_rate = round((len(municipality_rows) / municipality_population) * 1000, 2)
            municipality_analysis['bussiness_density_rate'] = bussiness_density_rate

        # Return formatted response identical to ActiveSearchController.get_pois_data
        return Response.success(data={
            "analysis_metrics": area_analysis,
            "analysis_metrics_municipality": municipality_analysis
        })
        
        
        
        
