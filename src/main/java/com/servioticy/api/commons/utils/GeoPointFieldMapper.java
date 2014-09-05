package com.servioticy.api.commons.utils;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.common.jackson.core.*;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper.MultiFields;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper.Defaults;
import org.elasticsearch.index.similarity.SimilarityProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.servioticy.api.commons.data.Group;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class GeoPointFieldMapper {
	private static org.elasticsearch.common.jackson.core.JsonFactory jsonFactory;
	private static Logger LOG = org.apache.log4j.Logger.getLogger(GeoPointFieldMapper.class);

	static {
        jsonFactory = new JsonFactory();
    }

    public static void parse(JsonNode root) throws IOException {
//    	ParseContext context = new ParseContext("location", null, null, null, null);
//
//    	JsonXContentParser parser = new JsonXContentParser(jsonFactory.createParser(root.asText()));
//    	context.reset(parser, null, null, null);

    	FieldMapper.Names names = new FieldMapper.Names("location"); // TODO location as Default.XXX
    	FieldType fieldType = new FieldType(Defaults.FIELD_TYPE);
    	Boolean docValues = null;
    	NamedAnalyzer indexAnalyzer = null;
    	NamedAnalyzer searchAnalyzer = null;
    	PostingsFormatProvider postingsFormat = null;
    	DocValuesFormatProvider docValuesFormat = null;
    	SimilarityProvider similarity = null;
    	Settings fieldDataSettings = null;
    	org.elasticsearch.common.settings.ImmutableSettings indexSettings = null; // ***
    	ContentPath.Type pathType = new ContentPath(1).pathType(); // ***
    	boolean enableLatLon = Defaults.ENABLE_LATLON;
    	boolean enableGeoHash = Defaults.ENABLE_GEOHASH;
    	boolean enableGeohashPrefix = Defaults.ENABLE_GEOHASH_PREFIX;
    	Integer precisionStep = null;
    	int geoHashPrecision = Defaults.GEO_HASH_PRECISION;
    	DoubleFieldMapper latMapper = null;
        DoubleFieldMapper lonMapper = null;
        StringFieldMapper geohashMapper = null;
        boolean validateLon = Defaults.VALIDATE_LON;
        boolean validateLat = Defaults.VALIDATE_LAT;
        boolean normalizeLon = Defaults.NORMALIZE_LON;
        boolean normalizeLat = Defaults.NORMALIZE_LAT;
        MultiFields multiFields = MultiFields.empty();

    	org.elasticsearch.index.mapper.geo.GeoPointFieldMapper mapper =
    			new org.elasticsearch.index.mapper.geo.GeoPointFieldMapper(names, fieldType, docValues,
    					indexAnalyzer, searchAnalyzer, postingsFormat, docValuesFormat, similarity, fieldDataSettings,
    					indexSettings, pathType, enableLatLon, enableGeoHash, enableGeohashPrefix, precisionStep,
    					geoHashPrecision, latMapper, lonMapper, geohashMapper, validateLon, validateLat, normalizeLon,
    					normalizeLat, multiFields);

    //<
//    	BytesArray bytesArray = new BytesArray(root.toString());
//    	SourceToParse source = new SourceToParse(SourceToParse.Origin.PRIMARY, bytesArray);
//    	XContentParser parser = XContentHelper.createParser(source.source());
    //||
    	JsonXContentParser parser = new JsonXContentParser(jsonFactory.createParser(root.toString()));
    	SourceToParse source = new SourceToParse(SourceToParse.Origin.PRIMARY, parser);
    //>

    	String index = "soupdates";
    	DocumentMapperParser docMapperParser = null;
    	DocumentMapper docMapper = null;

    	ParseContext context = new ParseContext(index, indexSettings, docMapperParser, docMapper, new ContentPath(1));
    	context.reset(parser, new ParseContext.Document(), source, null);

    	context.parser().nextToken(); // Start to process
    	try {
    		mapper.parse(context);
    	 } catch (Throwable e) {
		 LOG.error(e);
    		 throw new MapperParsingException("failed to parse geo_point", e);
    	 }
    }

    public static GeoPoint parseGeoPoint(JsonNode root, GeoPoint point) throws IOException, ServIoTWebApplicationException {

    	JsonXContentParser parser = new JsonXContentParser(jsonFactory.createParser(root.asText()));

    	org.elasticsearch.common.geo.GeoUtils.parseGeoPoint(parser, point);

    	return point;

    }

}
