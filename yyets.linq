<Query Kind="Statements">
  <NuGetReference>CsvHelper</NuGetReference>
  <NuGetReference>Dapper</NuGetReference>
  <NuGetReference>Newtonsoft.Json</NuGetReference>
  <NuGetReference>System.Data.SQLite.Core</NuGetReference>
  <Namespace>Dapper</Namespace>
  <Namespace>LINQPad.ObjectModel</Namespace>
  <Namespace>Newtonsoft.Json</Namespace>
  <Namespace>Newtonsoft.Json.Linq</Namespace>
  <Namespace>System.Data.SQLite</Namespace>
  <Namespace>System.Dynamic</Namespace>
  <Namespace>System.Text.Json</Namespace>
  <Namespace>CsvHelper</Namespace>
  <Namespace>System.Globalization</Namespace>
</Query>

var folder = Path.GetDirectoryName(Util.CurrentQueryPath);
Directory.GetFiles(folder).Dump();
var dbPath = Path.Combine(folder, "yyets.db").Dump();

var originPath = Path.Combine(folder, "yyets_origin.csv").Dump();
var cleanPath = Path.Combine(folder, "yyets_clean.csv").Dump();

using (var conn = new SQLiteConnection($"Data Source={dbPath};"))
{
	conn.Open();
	var rst = await conn.QueryAsync("select name,data from resource");

	var origin = from d in rst//.Take(10)
				 let jd = JsonConvert.DeserializeObject<dynamic>(d.data).data
				 //where jd.info.id.ToString() == "10053"
				 from jdl in (JArray)jd.list
				 from jdli in jdl["items"]
				 from jdliv in jdli.Values()
				 let resolution = ((JProperty)jdli).Name
				 let url = (JObject)jdliv["files"].FirstOrDefault()
				 where url != null
				 let files = new { Id = jd.info.id.ToString(), Name = $"{jd.info.cnname}({jd.info.enname})", FileName = jdliv["name"].ToString(), Size = jdliv["size"].ToString(), Resolution = resolution, Url = url["address"].ToString() }
				 //where files.Url.StartsWith("ed2k", true, CultureInfo.InvariantCulture) || files.Url.StartsWith("magnet", true, CultureInfo.InvariantCulture)
				 orderby files.Id descending, files.Resolution ascending, files.FileName ascending
				 select files;

	(from d in rst
	 let jd = JsonConvert.DeserializeObject<dynamic>(d.data).data
	 from jdl in (JArray)jd.list
	 from jdli in jdl["items"]
	 from jdliv in jdli.Values()
	 let resolution = ((JProperty)jdli).Name
	 let url = (JObject)jdliv["files"].FirstOrDefault()
	 select url?["address"].ToString().Split(':')[0]).Distinct().Dump("Url Type");

	var clean = origin.Where(p => p.Url.StartsWith("ed2k", true, CultureInfo.InvariantCulture) || p.Url.StartsWith("magnet", true, CultureInfo.InvariantCulture));

	var sum1 = $"剧集共计:{rst.Count()}".Dump();
	var sum2 = $"下载地址共计: {clean.Count()}(仅保留ed2k及magnet) / {origin.Count()}(原始数据)".Dump();
	//origin.Dump();

	using (var writer = new StreamWriter(originPath, false, Encoding.UTF8))
	using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
	{
		await csv.WriteRecordsAsync(origin);
	}

	using (var writer = new StreamWriter(cleanPath, false, Encoding.UTF8))
	using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
	{
		await csv.WriteRecordsAsync(clean);
	}
}