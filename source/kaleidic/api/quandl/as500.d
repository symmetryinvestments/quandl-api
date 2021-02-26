module kaleidic.api.quandl.as500;

import std.datetime: DateTime, Date, TimeOfDay;

DateTime[] quandlAS500CurrentTradingDays(string dir)
{
	import std.file;
	import std.algorithm:map,filter,sort;
	import std.array:array;
	import std.path: baseName, stripExtension;
	import std.string: isNumeric;

	return dirEntries(dir,"*.csv",SpanMode.depth)
		.filter!(entry=>entry.isFile && entry.name.baseName.stripExtension.isNumeric)
		.map!(entry=>entry.name.baseName.parseAS500Filename)
		.array
		.sort()
		.array;
}

DateTime parseAS500Filename(string filename)
{
	import std.string:endsWith;
	import std.exception: enforce;
	import std.conv: to;

	enforce(filename.length>=8 && filename.endsWith(".csv"));
	try
	{
		return DateTime(filename[0..4].to!int, filename[4..6].to!int, filename[6..8].to!int);
	}
	catch(Throwable t)
	{
		throw new Exception("parsing error for filename: "~filename~ ": "~t.to!string);
	}
}

DateTime[] quandlAS500MissingTradingDays(string apiKey, string dir)
{
	import std.array:array;
	import std.algorithm:sort,setDifference;
	auto currentDays = dir.quandlAS500CurrentTradingDays;
	auto availableDays = apiKey.quandlAS500AvailableTradingDays;
	return availableDays.setDifference(currentDays).array;
}

DateTime[] quandlAS500AvailableTradingDays(string apiKey)
{
	import std.zip;
	import std.net.curl;
	import std.array: appender, array;
	import std.algorithm: sort, map;
	import std.uri:encode;
	import std.stdio: writeln;
	import std.conv: to;
	import std.string: splitLines;

	enum urlFront = "https://www.quandl.com/api/v3/databases/AS500/download?api_key=";
	enum urlStub = "&download_type=all-trading-days";
	string url = urlFront ~ apiKey.encode ~ urlStub;
	writeln("connecting to : " ~ url);
	auto conn = HTTP();
	conn.handle.set(CurlOption.ssl_verifypeer, 0);
	auto rawZip = std.net.curl.get!(typeof(conn),ubyte)(url,conn);
	auto zip = new ZipArchive(rawZip);

	auto ret=appender!(DateTime[]);
	foreach(name, entry; zip.directory)
	{
		zip.expand(entry);
		ret.put((cast(string) entry.expandedData).splitLines.map!(line => DateTime(line[0..4].to!int, line[4..6].to!int, line[6..8].to!int)).array);
	}
	return ret.data.sort().array;
}

string[] quandlAS500TickerList(string apiKey, DateTime date)
{
	import std.zip;
	import std.net.curl;
	import std.array:appender;
	import std.format:format;
	import std.string: splitLines;

	enum urlFront = "https://www.quandl.com/api/v3/databases/AS500/download?api_key=";
	string urlStub = "&download_type=" ~ format("%04d%02d%02d",date.year, date.month, date.day ) ~ "-master";

	string url = urlFront ~ apiKey ~ urlStub;
	auto conn = HTTP();
	conn.handle.set(CurlOption.ssl_verifypeer, 0);
	auto rawZip = std.net.curl.get!(typeof(conn),ubyte)(url,conn);
	auto zip = new ZipArchive(rawZip);

	auto ret=appender!(string[]);
	foreach(name, entry; zip.directory)
	{
		zip.expand(entry);
		ret.put((cast(string) entry.expandedData).splitLines);
	}
	return ret.data;
}

void quandlAS500DownloadAll(string apiKey, string destination)
{
	import std.path:dirSeparator;
	import std.zip;
	import std.net.curl;
	import std.array:appender;
	import std.file: write;

	enum urlFront = "https://www.quandl.com/api/v3/databases/AS500/download?api_key=";
	enum urlStub = "&download_type=all-data";

	string url = urlFront ~ apiKey ~ urlStub;
	auto conn = HTTP();
	conn.handle.set(CurlOption.ssl_verifypeer, 0);
	auto rawZip = std.net.curl.get!(typeof(conn),ubyte)(url,conn);
	auto zip = new ZipArchive(rawZip);

	auto ret=appender!(DateTime[]);
	foreach(name, entry; zip.directory)
	{
		zip.expand(entry);
		auto path = destination ~ dirSeparator ~ name;
		write(name,entry.expandedData);
	}
}

void quandlAS500DownloadDay(string apiKey, DateTime date, string destination)
{
	import std.path:dirSeparator;
	import std.zip;
	import std.net.curl;
	import std.array:appender;
	import std.format:format;
	import std.experimental.logger;
	import std.process:execute;
	import std.conv: to;
	import std.file: write, remove;
	import std.exception: enforce;

	tracef("downloading Quandl AS500 for %s",date);
	enum urlFront = "https://www.quandl.com/api/v3/databases/AS500/download?api_key=";
	string urlStub = "&download_type=" ~ format("%04d%02d%02d",date.year, date.month, date.day);

	string url = urlFront ~ apiKey ~ urlStub;
	tracef("downloading %s",url);
	auto conn = HTTP();
	conn.handle.set(CurlOption.ssl_verifypeer, 0);
	auto rawZip = std.net.curl.get!(typeof(conn),ubyte)(url,conn).to!(void[]);

	/**
		std.zip fails for some reason with overflow when unzipping Quandl AS500 single day files
	*/
/*	auto zip = new ZipArchive(rawZip);

	foreach(name, entry; zip.directory)
	{
		tracef("expanding %s",name);
		zip.expand(entry);
		tracef("expandedSize=%s; crc32=%s",name,entry.expandedSize, entry.crc32);
		auto path = destination ~ dirSeparator ~ name;
		tracef("writing %s to %s",name,path);
		std.file.write(name,entry.expandedData);
	}*/
	auto destFile=destination ~dirSeparator ~ "$temp_quandlAS500.zip";
	write(destFile,rawZip);
	auto result = execute(["unzip",destFile,"-d",destination]);
	enforce(result.status==0,"unzip failed for "~date.to!string);
	remove(destFile);
}

private DateTime toLine(char[] date, char[] time)
{
	import std.conv:to;
	int year = date[0 .. 4].to!int;
	int month = date[4..6].to!int;
	int day = date[6..8].to!int;
	int hour = time[0..2].to!int;
	int minute = time[3..5].to!int;
	int second = time[6..8].to!int;
	return DateTime(Date(day,month,year),TimeOfDay(hour,minute,second));
}

private struct PriceBar
{
	DateTime date;
	double open;
	double high;
	double low;
	double close;
	long volume;
}

private string remapTicker(string ticker)
{
	import std.array: replace;
	return ticker.replace("/","_");
}

void processBars(DateTime[] dates, string dir)
{
	import kaleidic.api.snappyd: snappyCompress, snappyUncompress;
	import std.conv:to;
	import std.experimental.logger;
	import std.algorithm: map, filter, sort, splitter;
	import std.range:enumerate, dropOne, drop;
	import std.format:format;
	import std.array: Appender, appender, array;
	import std.file: dirEntries, SpanMode, read, write;
	import std.path: dirSeparator, baseName, stripExtension;
	import std.stdio: stderr, File;
	import core.memory:GC;

	Appender!(PriceBar[])[string] bars;

	if (dates.length==0)
	{
		info("no new dates to download");
		return;
	}
	tracef("reading all bars in");
	foreach(i,file;enumerate(dirEntries(dir,"*.bin",SpanMode.depth).filter!(entry=>(entry.isFile)).array.sort!((a,b )=> (a.name<b.name))))
	{
		auto ticker=file.name.baseName.stripExtension;
		bars[ticker]=appender!(PriceBar[]);
		bars[ticker] ~= (cast(ubyte[]) read(file.name)).snappyUncompress!(PriceBar[]);
		if (i%10==0)
			stderr.write(".");
	}
	stderr.writeln("");
	foreach(file;dates.map!(date => dir ~ dirSeparator ~ format("%04d%02d%02d.csv",date.year, date.month, date.day)))
	{
		auto fileContents=File(file);
		foreach(line;fileContents.byLine.dropOne)
		{
			auto cols=line.splitter(',');
			auto lineDate = toLine(cols.front,cols.dropOne.front);
			auto ticker = cols.drop(2).front.to!string.remapTicker;
			PriceBar bar;
			bar.date = lineDate;
			bar.open = cols.drop(3).front.to!double;
			bar.high = cols.drop(4).front.to!double;
			bar.low = cols.drop(5).front.to!double;
			bar.close = cols.drop(6).front.to!double;
			bar.volume= cols.drop(8).front.to!long;
			auto p = ticker in bars;
			if (p is null)
			{
				bars[ticker] = appender!(PriceBar[]);
				bars[ticker].put(bar);
			}
			else
			{
				(*p).put(bar);
			}
		}
	}

	foreach(i,ticker;enumerate(bars.keys.sort()))
	{
		if (i%10==0)
			stderr.write(".");
	    auto uncompressedData=cast(ubyte[])bars[ticker].data;
		write(dir~dirSeparator~ticker~".bin",uncompressedData.snappyCompress);
		bars[ticker].clear();
		GC.collect();
	}
	stderr.writeln("");
}

auto quandlAS500DownloadMissingTradingDays(string apiKey, string dir)
{
	auto missingDates = quandlAS500MissingTradingDays(apiKey, dir);
	foreach(entry; missingDates)
		quandlAS500DownloadDay(apiKey, entry, dir);
	return missingDates;
}
