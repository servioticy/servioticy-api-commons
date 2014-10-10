package com.servioticy.api.commons.elasticsearch;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.index.query.GeoDistanceFilterBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.data.Group;

public class SearchCriteria {

	private static Logger LOG = org.apache.log4j.Logger.getLogger(SearchCriteria.class);

    private static ObjectMapper mapper = new ObjectMapper();

    // RANGES ***********************************
    public boolean  		timerange = false;
    public boolean  		numericrange = false;
    // GENERIC
    public double           rangefrom = Double.MIN_VALUE;
    public double           rangeto = Double.MAX_VALUE;
    // ONLY FOR GENERICRANGE
    public String           numericrangefield;

    // LIMIT ***********************************
    public boolean  		limit = false;
    public int      		limitcount;

    // GEODISTANCE *****************************
    public boolean  		geodistance = false;

    // GENERIC
    public double           pointlat;
    public double           pointlon;

    // DISTANCE
    public double           geodistancevalue;
    public String           geodistanceunit = "km";


    // GEOSHAPE ********************************
    public boolean  		geoboundingbox = false;

    // BOX
    public double           geoboxupperleftlat;
    public double           geoboxupperleftlon;
    public double           geoboxbottomrightlat;
    public double           geoboxbottomrightlon;


    // MATCH ********************************
    public boolean  		match = false;
    public String           matchfield;
    public String           matchstring;

    public static SearchCriteria buildFromJson(String searchCriteriaJson) {

        try {
            System.out.println("Building from: --" + searchCriteriaJson + "--");
            SearchCriteria res = mapper.readValue(searchCriteriaJson, SearchCriteria.class);
            return res;

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Returns true if combination is possible
    public boolean valid() {
        return
                (timerange || (numericrange && numericrangefield != null) ||
                (timerange && numericrange && numericrangefield != null &&
                        !numericrangefield.contains("couchbaseDocument.doc.lastUpdate")) ||
                geodistance  ^ geoboundingbox) ||
                (match && (matchfield != null && matchstring != null))
        ;
    }

    public String buildFilter() {

        StringBuilder filter = new StringBuilder();

        if(!valid())
            return null;

        AndFilterBuilder global = FilterBuilders.andFilter();

        if(timerange) {
            RangeFilterBuilder rangeFilter =
            		FilterBuilders.rangeFilter("doc.lastUpdate")
                                  .from((long)rangefrom).to((long)rangeto)
                                  .includeLower(true).includeUpper(true);
            //filter.append(rangeFilter.toString());
            global.add(rangeFilter);
        }

        if(numericrange) {

            RangeFilterBuilder numericrangeFilter =
            		FilterBuilders.rangeFilter("doc."+numericrangefield)
                                  .from(rangefrom).includeLower(true)
                                  .to(rangeto).includeUpper(true);

            //filter.append(numericrangeFilter());
            global.add(numericrangeFilter);
        }

        if(geodistance) {

            GeoDistanceFilterBuilder geodistanceFilter =
            		FilterBuilders.geoDistanceFilter("doc.channels.location.current-value")
            					  .distance(geodistancevalue, DistanceUnit.fromString(geodistanceunit))
                                  .point(pointlat,pointlon);

            //filter.append(geodistanceFilter.toString());
            global.add(geodistanceFilter);
        }

        if(geoboundingbox) {

            GeoBoundingBoxFilterBuilder geodbboxFilter =
            		FilterBuilders.geoBoundingBoxFilter("doc.channels.location.current-value")
                                  .topLeft(geoboxupperleftlat, geoboxupperleftlon)
                                  .bottomRight(geoboxbottomrightlat, geoboxbottomrightlon);


            //filter.append(geodbboxFilter());
            global.add(geodbboxFilter);
        }


        if(match) {

            TermFilterBuilder matchFilter = FilterBuilders.termFilter("doc." + matchfield, matchstring);

            //filter.append(matchFilter.toString());
            global.add(matchFilter);
        }

        filter.append(global.toString());

        return filter.toString();
    }


    public String toString() {

        StringBuilder res = new StringBuilder();
        for (Field field : this.getClass().getDeclaredFields()) {
            try {
                res.append(field.getName() + ": " + field.get(this)+"\n");
            } catch (IllegalArgumentException e) {
                LOG.error(e);
            } catch (IllegalAccessException e) {
		LOG.error(e);
            }
        }

        return res.toString();
    }
}
