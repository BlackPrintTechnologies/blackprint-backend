from utils.iconUtils import IconMapper
import logging

logger = logging.getLogger(__name__)


def format_pois_data(pois_data,total_population=0):
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
