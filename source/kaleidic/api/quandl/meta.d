/**

	Quandl Util:

*/

module kaleidic.api.quandl.meta;
import vibe.data.json: optional, Json;
import std.datetime : DateTime, Date, TimeOfDay;


struct MetaSummary
{
	long[string] numTickers;
	long[string] numPoints;
}
/**
	crude - does not account for partial periods
	should implement diff functions in kaleidic.dates that returns a tuple of whole number and modulus units
	eg returns x weeks, y days
*/

long countPoints(Date startDate, Date endDate, string frequency)
{
    import std.string: toLower;
	import std.conv : to;

	switch(frequency.toLower)
	{
		case "daily":
			return (endDate - startDate).total!"days";
		case "weekly":
			return (endDate - startDate).total!"weeks";
		case "monthly":
			return ((endDate - startDate).total!"weeks"/4.33).to!long;
		case "quarterly":
			return (endDate - startDate).total!"weeks" * 4 / 52;
		case "annual":
			return (endDate - startDate).total!"weeks" / 52;
		default:
			throw new Exception("unknown frequency: "~frequency);
	}
	assert(0);
}

MetaSummary getMetaSummary(string dataSource)
{
    import std.file: read;
    import std.stdio: stderr;

	MetaSummary ret;
	auto filename=dataSource~".json";
	auto jsonBuf=cast(string)read(filename);
	auto metadata=getMetaDataFromString(jsonBuf);
	foreach(entry;metadata.docs)
	{
		auto frequency=entry.frequency.stripQuote;
		++ret.numTickers[frequency];
		if ((entry.from_date.stripQuote.length!=10)||((entry.from_date.stripQuote.length!=10)))
			continue;
		try
		{
			//stderr.writefln("ticker: %s, trying to parse dates: %s,%s,%s",entry.code, entry.from_date, entry.to_date, entry.frequency);
			//stderr.writefln("or %s,%s,%s,%s",entry.code.stripQuote, entry.from_date.stripQuote, entry.to_date.stripQuote, frequency);
			//stderr.flush;
			ret.numPoints[frequency]+=
				countPoints(parseDate(entry.from_date.stripQuote),
							parseDate(entry.to_date.stripQuote),	frequency);
		}
		catch (Exception e)
		{
			stderr.writefln("was trying to parse dates: %s,%s,%s",entry.from_date, entry.to_date, frequency);
			stderr.writefln("%s",e.msg);
			stderr.writefln("%s",entry);
			stderr.writefln("\n");
			stderr.flush;
			// throw new Exception(e.msg);
		}
	}
	return ret;
}

private DateTime parseDateTime(string s)
{
	import std.conv : to;
	auto year = s[0 .. 4].to!int;
	auto month = s[5 .. 7].to!int;
	auto day = s[8 .. 10].to!int;
	auto hour = s[11 .. 13].to!int;
	auto minute = s[14 .. 16].to!int;
	auto sec = s[17 .. 19].to!int;
	return DateTime(Date(year,month,day),TimeOfDay(hour,minute,sec));
}

private Date parseDate(string s)
{
	import std.conv : to;
	auto year = s[0 .. 4].to!int;
	auto month = s[5 .. 7].to!int;
	auto day = s[8 .. 10].to!int;
	return Date(year,month,day);
}

// add error handling / validation so don't choke on bad date
// handle range error
DateTime quandlDateToDateTime(string quandlDate)
{
    import std.algorithm: canFind;
    import std.conv: to;

	quandlDate=quandlDate.stripQuote;
	if (quandlDate.canFind("T") || quandlDate.canFind(":"))
	{
		auto dt=quandlDate.parseDateTime;
		return DateTime(dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second.to!int);
	}
	else
	{
		auto date=quandlDate.parseDate;
		return DateTime(date.year,date.month,date.day);
	}
	assert(0);
}
string stripQuote(string s)
{
    import std.array: replace;
	return s.replace("\\\"","").replace("\"","");
}

struct ParsedMetaDataTable
{
    import vibe.data.json: optional, Json;

    @optional Json spellcheck;
    @optional int start;
    int totalCount;
    int currentPage;
    int perPage;

    @optional Json highlighting;
    Json docs;
    @optional Json sources;
}

struct MetaDataEntry
{
   int id;
   string description;
   bool premium;
   bool private_;
   string urlize_name;
   string code;
   string refreshed_at;
   string type;
   string to_date;
   string from_date;
   string name;
   string frequency;
   string created_at;
   string source_code;
   string source_id;
   string display_url;
   string updated_at;
   string[] column_names;
}

struct MetaDataTable
{
   string spellcheck;
   int start;
   int totalCount;
   int currentPage;
   int perPage;

   string[] highlighting;
   MetaDataEntry[] docs;
   string[] sources;
}

struct JsonDataTable
{
   @optional Json spellcheck;
   @optional int start;
   int totalCount;
   int currentPage;
   int perPage;

   @optional Json highlighting;
   Json docs;
   @optional Json sources;
}


MetaDataTable getMetaDataFromString(string jsonBuf)
{
    import vibe.data.json: parseJson;
    import std.stdio: writefln;

	MetaDataTable table;
	//auto results = deserializeJson!(JsonStringSerializer!string, ParsedMetaDataTable)(jsonBuf);
	auto results=jsonBuf.parseJson;

	table.docs.reserve(results["docs"].length);
	table.spellcheck=results["spellcheck"].to!string;
	table.start=results["start"].get!int;
	table.totalCount=results["totalCount"].get!int;
	table.currentPage=results["currentPage"].get!int;
	table.perPage=results["perPage"].get!int;

	int i=0;

	foreach(row;results["docs"])
	{
		writefln("%s,%s",i,row["code"].get!string);
		MetaDataEntry ret;
		ret.id=row["id"].get!int;
		ret.description=row["description"].get!string;
		ret.premium=row["premium"].get!bool;
		ret.urlize_name=row["urlize_name"].get!string;
		ret.code=row["code"].get!string;
		ret.refreshed_at=row["refreshed_at"].get!string;
		ret.type=row["type"].get!string;
		ret.to_date=row["to_date"].get!string;
		ret.from_date=row["from_date"].get!string;
		ret.name=row["name"].get!string;
		ret.frequency=row["frequency"].get!string;
		ret.created_at=row["created_at"].get!string;
		ret.source_code=row["source_code"].get!string;
		ret.source_id=row["source_id"].get!string;
		ret.display_url=row["display_url"].get!string;
		ret.updated_at=row["updated_at"].get!string;
		//ret.column_names=
		table.docs~=ret;
		++i;
	}
	destroy(results);
	//GC.collect();
	return table;
}

struct QuandlSeriesTable
{
    import vibe.data.json: optional, Json;
	string code;
	//@name("private")
	bool privateX;
	@optional Json errors;
	int id;
	string description;

	@optional string source_code;
	@optional string source_name;
	@optional string name;
	@optional string urlize_name;
	@optional string display_url;
	@optional string updated_at;
	@optional string frequency;
	@optional string from_date;
	@optional string to_date;
	Json column_names;
	@optional string type;
	@optional bool premium;
	Json data;

}
