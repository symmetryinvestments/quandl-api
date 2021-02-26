module kaleidic.api.quandl.lookup;

/++
import std.stdio;
imoprt std.file;

string[2][] lookupTickers(string dataSource,string[] searchItems)
{
	import stdx.data.json;
	import std.conv:to;
	import std.algorithm:canFind,countUntil,joiner,map;
	import std.string:toLower;
	bool found=false;
	bool checkedCode=false;
	bool checkedName=false;
	string[2][] ret;
	string buf;
	string code=null;
	auto filename="../importquandl/"~dataSource~".json";
	//auto data=cast(string)std.file.read(filename);
	auto data = File("fileName")
		.byChunk(100*1024 * 1024) //1 MB. Data cluster equals 1024 * 4
		// .map!(ch => ch.idup)
		.joiner
		.map!(b => cast(char)b);
	auto range1=parseJSONStream(data);
	string seriesName=null;
	string lastKey=null;
	int depth=0;
	ret.reserve(filename.getSize/500);
	foreach(entry;range1)
	{
		switch(entry.kind) with (JSONParserNode.Kind)
		{
			case objectStart:
				++depth;
				if(depth<=3)
				{
					buf="";
					found=false;
					code=null;
					checkedCode=false;
					checkedName=false;
				}
				break;
			case objectEnd:
				seriesName=null;
				--depth;
				checkedCode=false;
				checkedName=false;
				buf=null;
				if((depth==2) && found)
				{
					if (code is null)
						code="NULL";
					if (buf is null)
						buf="NULL";
					ret~=[code,buf];
					found=false;
				}
				break;
			case arrayStart:
				++depth;
				seriesName=lastKey;
				lastKey=null;
				break;
			case arrayEnd:
				--depth;
				seriesName=null;
				break;
			case key:
				if (seriesName==null)
					seriesName=entry.toString;
				lastKey=entry.key;
				break;
			case literal:
				switch(entry.literal.kind) with (JSONToken.Kind)
				{
					case string:
						if (!checkedCode || !checkedName)
						{
							buf~=lastKey~":"~entry.literal.string.stripQuotes~"\n";
							if (lastKey=="code")
							{
								code=entry.literal.string.stripQuotes;
								checkedCode=true;
							}
							if (lastKey=="name")
								checkedName=true;

							if ((lastKey=="code") || (lastKey=="name"))
							{
								if (searchItems.canFind!"(b.indexOf(a.toLower)>=0)"(entry.literal.string.stripQuotes.toLower))
									found=true;
								else if (checkedCode && checkedName)
									found=false;
							}
						}
						else if (found)
							buf~=lastKey~":"~entry.literal.string.stripQuotes~"\n";

						break;
					default:
						buf~=entry.toString~"\n";
						break;
				}
				lastKey=null;
				break;
			default:
				break;
		}
	}
	return ret;
}

string stripQuotes(string s)
{
	if (s[0]=='\"')
		s=s[1..$];
	if (s[$-1]=='\"')
		s=s[0..$-1];
	return s;
}
+/
