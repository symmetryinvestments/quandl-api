/**
	Enhanced version to deal with rate limiting and be friendly to asynchronous requests - multiple threads downloading should
	jointly respect API limit.  But should be possible to have multiple tokens running simultaneously

*/
module kaleidic.api.quandl.quandl;

import std.typecons: Tuple;


enum QuandlAPIKey="";


enum Quandl
{
	metadata="https://www.quandl.com/api/v2/datasets.%s?query=*&source_code=%s&per_page=300&page=%s",
	data = "https://www.quandl.com/api/v3/datasets/%s/%s.%s?sort_order=%s",
	search = "https://www.quandl.com/api/v1/datasets.json?query=%s&per_page=%s",
}

struct APILimit
{
	int numMinutes;
	int numRequestsAllowed;
}

struct QuandlAPI
{
	import std.datetime: DateTime, Duration, dur;

	string token=QuandlAPIKey;
	APILimit[][string] apiLimitsSearch; //=		["":[APILimit(10,60)]];
	APILimit[][string] apiLimitsGet; //=		["":[APILimit(10,2_000)]];

	DateTime[][string] getDateTimes;
	DateTime[][string] searchDateTimes;

	this(string token)
	{
		this.token=token;
		apiLimitsSearch=	[this.token: [APILimit(10,60)]];
		apiLimitsGet=		[this.token: [APILimit(10,1_800)]];
	}
	this(string token, APILimit[] getLimits, APILimit[] searchLimits)
	{
		this.token=token;
		this.apiLimitsGet[this.token]=getLimits;
		this.apiLimitsSearch[this.token]=searchLimits;
	}

	void logGet(DateTime dt)
	{
		getDateTimes[this.token]~=dt;
	}

	void logSearch(DateTime dt)
	{
		searchDateTimes[this.token]~=dt;
	}

	size_t countRequestsWindowGet(Duration dur)
	{
		import std.datetime: Clock;
		import std.algorithm: count;

		auto p=this.token in getDateTimes;
		if (!p || p.length==0)
			return 0;
		auto now=cast(DateTime)Clock.currTime;
		return (*p).count!(a=>((now-a)<dur));
	}

	size_t countRequestsWindowSearch(Duration dur)
	{
		import std.datetime: Clock;
		import std.algorithm: count;

		auto p=this.token in searchDateTimes;
		if (!p || p.length==0)
			return 0;
		auto now=cast(DateTime)Clock.currTime;
		return (*p).count!(a=>((now-a)<dur));
	}

	bool canProceedGet()
	{
		bool ret=true;
		foreach(limit;this.apiLimitsGet[token])
		{
			ret&=(this.countRequestsWindowGet(dur!"minutes"(limit.numMinutes))<limit.numRequestsAllowed);
		}
		return ret;
	}

	bool canProceedSearch()
	{
		bool ret=true;
		foreach(limit;this.apiLimitsSearch[token])
		{
			ret&=(this.countRequestsWindowSearch(dur!"minutes"(limit.numMinutes))<limit.numRequestsAllowed);
		}
		return ret;
	}
	bool canProceedThenLogGet()
	{
		import std.datetime: Clock;
		bool ret=true;
		foreach(limit;apiLimitsGet[token])
		{
			ret&=(this.countRequestsWindowGet(dur!"minutes"(limit.numMinutes))<limit.numRequestsAllowed);
		}
		if(ret)
			this.logGet(cast(DateTime)Clock.currTime);
		return ret;
	}
	bool canProceedThenLogSearch()
	{
		import std.datetime: Clock;
		bool ret=true;
		foreach(limit;apiLimitsSearch[token])
		{
			ret&=(this.countRequestsWindowSearch(dur!"minutes"(limit.numMinutes))<limit.numRequestsAllowed);
		}
		if(ret)
			this.logSearch(cast(DateTime)Clock.currTime);
		return ret;
	}

	string get(string code, string ticker, string order, string StartDate, string EndDate, string transformation, string collapse, string rows, string filetype,
				Duration timeout=dur!"minutes"(15))
	{
		import std.string: toLower;
		import std.format: format;
		import std.stdio: writefln, stdout, stderr;
		import std.datetime: Clock;
		import std.net.curl: HTTP, CurlOption;
		import std.conv: to;
		static import std.net.curl;

		filetype=filetype.toLower;
		order = order.toLower;
		if (order.length==0)
			order="asc";
		if (filetype.length==0)
			filetype="json";

		string url = format(Quandl.data,code,ticker,filetype ,order);
		if (this.token.length == 0) {
			writefln(	"It appear you are not using an authentication" ~
						" token. Please visit http://www.quandl.com/help/api for getting one" ~
						" ; otherwise your usage may be limited.");
		}
		else
			url ~= "&auth_token=" ~ this.token;
		if (StartDate.length>0)
			url ~= "&trim_start=" ~ StartDate;
		if (EndDate.length>0)
			url ~= "&trim_end=" ~ EndDate;
		if (transformation.length>0)
			url ~= "&transformation=" ~ transformation;
		if (collapse.length>0)
			url ~= "&collapse=" ~ collapse;
		if (rows.length>0)
			url ~= "&rows=" ~ rows;
		writefln("URL=%s",url);
		stdout.flush;
		auto conn = HTTP();
	  	conn.handle.set(CurlOption.ssl_verifypeer, 0);
	  	auto now=Clock.currTime;
	  	while ((Clock.currTime-now)<timeout)
	  	{
		  	if (this.canProceedThenLogGet())
		  	{
		  		return std.net.curl.get(url,conn).to!string;
		  	}
		  	stderr.writefln("* waiting");
		  	stderr.flush;
	  	}
	  	throw new Exception("timed out whilst waiting for API limit to download ticker: "~code);
	}

	string get(string code,string ticker,string type)
	{
		return get(code,ticker,"","","","","","",type);
	}


	string search(string query, long maxresults, Duration timeout)
	{
		import std.format: format;
		import std.stdio: stderr;
		import std.datetime: Clock;
		import std.net.curl: HTTP, CurlOption;
		import std.conv: to;
		static import std.net.curl;

		auto url=format(Quandl.search,query,maxresults); // should replace whitespace in query with +
		if (this.token.length == 0)
		{
			stderr.writeln(	"It appear you are not using an authentication",
								" token. Please visit http://www.quandl.com/help/api for getting one",
						" ; otherwise your usage may be limited.");
		}
		else
			url ~= "&auth_token=" ~ this.token;

		auto conn = HTTP();
	  	conn.handle.set(CurlOption.ssl_verifypeer, 0);
	  	auto now=Clock.currTime;
	  	while ((Clock.currTime-now)<timeout)
	  	{
		  	if (this.canProceedThenLogSearch())
		  	{
		  		return std.net.curl.get(url,conn).to!string;
		  	}
	  	}
	  	throw new Exception("timed out whilst waiting for API limit to search for: "~query);
	}

	string getMetaData(string dataSource, string type="json", int page=1)
	{
		import std.string: toLower;
		import std.stdio: stderr, writefln;
		import std.format: format;
		import std.net.curl: HTTP, CurlOption;
		import std.conv: to;
		static import std.net.curl;

		type=type.toLower;
		string url = format(Quandl.metadata, type, dataSource,page);

		if (this.token.length == 0)
		{
			stderr.writeln(	"It would appear you are not using an authentication ",
					 		 	"token. Please visit http://www.quandl.com/help/c++ ",
								"or your usage may be limited.\n");
		}
		else {
			url ~= "&auth_token=" ~ this.token;
		}

		string fileName;
		auto iLength = dataSource.length;
		foreach(i;0 .. iLength)
		{
			if (dataSource[i] == '/')
			{
				fileName = dataSource[i+1.. iLength];
				break;
			}
		}
		fileName~="."~type;
		debug{
			writefln("%s,%s", iLength,fileName);
			writefln(url);
		}
		auto conn = HTTP();
	  	conn.handle.set(CurlOption.ssl_verifypeer, 0);
		return std.net.curl.get(url,conn).to!string;
	}
}

version(StandAlone)
{
	void main(string[] args)
	{
		auto quandl=QuandlAPI("qw6JYQqogKj44KGpzyo1");
		writefln("%s",quandl.get("BAVERAGE","ANX_HKUSD","csv"));
		writefln("%s", quandl.get("BAVERAGE","ANX_HKUSD","asc","1990-01-01","2015-01-01","","","","json"));
		writefln("****");
		writefln("%s",quandl.getDateTimes["qw6JYQqogKj44KGpzyo1"]);
	}
}
