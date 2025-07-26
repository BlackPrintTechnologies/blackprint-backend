# API Integration Report for New City (Querétaro)

## Overview
This report outlines all the APIs that need to be modified to integrate the new city (Querétaro) data. The integration involves changing from the current `v_parcel_v3` table to the new `v_qro` table and related dimension tables.

## Current vs New Table Structure

### Current Tables (CDMX):
- **Main View**: `blackprint_db_prd.data_product.v_parcel_v3`
- **POI Data**: `blackprint_db_prd.presentation.dim_places`
- **Market Data**: 
  - `blackprint_db_prd.presentation.dim_market_data_spot2`
  - `blackprint_db_prd.presentation.dim_market_data_inmuebles24`
  - `blackprint_db_prd.presentation.dim_market_data_propiedades`

### New Tables (Querétaro):
- **Main View**: `blackprint_db_prd.data_product.v_qro`
- **POI Data**: `blackprint_db_prd.presentation.dim_places_qro`
- **Market Data**: 
  - `blackprint_db_prd.presentation.dim_market_data_spot2_qro`
  - `blackprint_db_prd.presentation.dim_market_data_inmuebles24_qro`

## APIs Requiring Changes

### 1. PROPERTIES MODULE (HIGH PRIORITY)

#### 1.1 Property Controller (`module/properties/controller.py`)
**File**: `module/properties/controller.py`
**Lines**: 265-1475
**Changes Required**:
- Update all database queries to use `v_qro` instead of `v_parcel_v3`
- Modify `get_properties()` method
- Update `get_property_demographic()` method
- Update `get_property_traffic()` method
- Update `filter_properties()` method
- Update `get_property_market_info()` method

#### 1.2 Property Query Controller (`module/properties/query.py`)
**File**: `module/properties/query.py`
**Lines**: 1-980
**Changes Required**:
- Update `get_property_query()` method to use `v_qro`
- Update `get_demographics_query()` method
- Update `get_commercial_growth_query()` method
- Update `get_market_info_query()` method to use new dimension tables

#### 1.3 Property Routes (`module/properties/routes.py`)
**File**: `module/properties/routes.py`
**Lines**: 1-259
**APIs Affected**:
- `POST /property` - Property search and filtering
- `GET /property/demographic` - Demographic data
- `GET /property/marketinfo` - Market information
- `GET /property/commercial-growth` - Commercial growth data
- `POST /property/filter` - Advanced property filtering
- `POST /properties/municipality_search` - Municipality search

### 2. LAYERS MODULE (HIGH PRIORITY)

#### 2.1 Layer Controller (`module/layers/controller.py`)
**File**: `module/layers/controller.py`
**Lines**: 1-218
**Changes Required**:
- Update `get_property_query()` method
- Update `get_brands()` method to use `dim_places_qro`
- Update `get_mobility_data_within_buffer()` method
- Update all POI-related queries

#### 2.2 Layer Routes (`module/layers/routes.py`)
**File**: `module/layers/routes.py`
**Lines**: 1-113
**APIs Affected**:
- `POST /brands` - Brand information
- `GET /searchbrands/` - Brand search
- `POST /traffic` - Traffic data
- `GET /property/layer` - Property layer data

#### 2.3 Traffic UDF (`module/layers/udfs/traffic_udf.py`)
**File**: `module/layers/udfs/traffic_udf.py`
**Changes Required**:
- Update mobility data queries to use new table structure

### 3. SEARCH MODULE (MEDIUM PRIORITY)

#### 3.1 Search Controller (`module/search/controller.py`)
**File**: `module/search/controller.py`
**Lines**: 1-177
**Changes Required**:
- Update any property-related search queries
- Modify saved search functionality if it includes property data

#### 3.2 Search Routes (`module/search/routes.py`)
**File**: `module/search/routes.py`
**Lines**: 1-87
**APIs Affected**:
- `GET /savesearch` - Saved searches
- `POST /savesearch` - Create saved search
- `PUT /savesearch` - Update saved search
- `DELETE /savesearch` - Delete saved search

### 4. GROUP MODULE (MEDIUM PRIORITY)

#### 4.1 Group Controller (`module/group/controller.py`)
**File**: `module/group/controller.py`
**Lines**: 1-281
**Changes Required**:
- Update property-related group operations
- Modify property ID handling if FID structure changes

#### 4.2 Group Routes (`module/group/routes.py`)
**File**: `module/group/routes.py`
**Lines**: 1-87
**APIs Affected**:
- `GET /group` - Get groups
- `POST /group` - Create group
- `PUT /group` - Update group
- `DELETE /group` - Delete group
- `POST /groupproperty` - Add properties to group
- `DELETE /groupproperty` - Remove properties from group

### 5. USER MODULE (LOW PRIORITY)

#### 5.1 User Controller (`module/user/controller.py`)
**File**: `module/user/controller.py`
**Lines**: 1-430
**Changes Required**:
- Minimal changes unless user preferences include city-specific data

#### 5.2 User Routes (`module/user/routes.py`)
**File**: `module/user/routes.py`
**Lines**: 1-310
**APIs Affected**:
- No direct changes required unless user preferences include city data

## Implementation Priority Order

### Phase 1: Core Property APIs (Week 1)
1. **Property Query Controller** - Update all SQL queries
2. **Property Controller** - Update main property methods
3. **Property Routes** - Test all property endpoints

### Phase 2: Layer and Traffic APIs (Week 2)
1. **Layer Controller** - Update POI and brand queries
2. **Traffic UDF** - Update mobility data queries
3. **Layer Routes** - Test layer endpoints

### Phase 3: Supporting APIs (Week 3)
1. **Search Module** - Update saved searches
2. **Group Module** - Update property groups
3. **User Module** - Any city-specific user data

## Key Changes Required

### 1. Database Schema Changes
- Replace `v_parcel_v3` with `v_qro`
- Replace `dim_places` with `dim_places_qro`
- Replace market data tables with `_qro` versions

### 2. Column Mapping
The new `v_qro` table has different column names and structure:
- Primary key: `id_stg_demographic_socioeconomic_qro` instead of `fid`
- New demographic columns with different naming conventions
- Additional POI and traffic data columns
- New distance-based columns (100m, 200m, 250m)

### 3. Query Updates
- Update all SELECT statements
- Modify JOIN conditions
- Update WHERE clauses
- Adjust column references

### 4. Response Format Changes
- Update JSON response structures
- Modify data normalization
- Update caching keys

## Testing Strategy

### Unit Tests
- Update existing test files in `tests/` directory
- Create new tests for Querétaro-specific functionality
- Test all modified controllers and routes

### Integration Tests
- Test complete API workflows
- Verify data consistency
- Test caching mechanisms

### Performance Tests
- Monitor query performance
- Test with large datasets
- Verify response times

## Risk Assessment

### High Risk
- Data structure differences between tables
- Column name mismatches
- Performance impact of new queries

### Medium Risk
- Caching invalidation
- API response format changes
- User experience impact

### Low Risk
- Authentication/authorization
- Basic CRUD operations

## Rollback Plan

1. **Database Level**: Keep both table structures during transition
2. **API Level**: Implement feature flags for city selection
3. **Application Level**: Maintain backward compatibility
4. **Monitoring**: Implement comprehensive logging and monitoring

## Success Criteria

1. All APIs return correct data for Querétaro
2. No performance degradation
3. All existing functionality preserved
4. Comprehensive test coverage
5. Documentation updated
6. Monitoring and alerting in place

## Next Steps

1. **Immediate**: Start with Property Query Controller updates
2. **Week 1**: Complete Phase 1 implementation
3. **Week 2**: Complete Phase 2 implementation
4. **Week 3**: Complete Phase 3 implementation
5. **Week 4**: Testing and optimization
6. **Week 5**: Deployment and monitoring

## Notes

- All changes should be made incrementally
- Each API should be tested individually
- Maintain backward compatibility where possible
- Document all changes thoroughly
- Implement proper error handling for new data structures 