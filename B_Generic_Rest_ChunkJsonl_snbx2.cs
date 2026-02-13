using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Storage.Blobs;

namespace Ingestion.Functions;

/// <summary>
/// SNBX2 processor: fetch up to ChunkPageCount pages per function call, and write ONE JSONL file per chunk:
///   {BlobFolderPrefix}/chunk_0001.jsonl
/// JSONL content: one NEO object per line (raw JSON), extracted from $.near_earth_objects per page.
/// </summary>
public sealed class B_Generic_Rest_ChunkJsonl_snbx2 : IChunkProcessor
{
    public string Name => "B_Generic_Rest_ChunkJsonl_snbx2";

    public async Task<ChunkStepResult> ExecuteAsync(ChunkIngestContext ctx, CancellationToken ct)
    {
        // Blob connection string
        var blobConn = Environment.GetEnvironmentVariable("BLOB_CONN_STR")
                      ?? throw new InvalidOperationException("Missing env var BLOB_CONN_STR");

        // Parse config JSON
        var auth = ParseJson(ctx.AuthConfigJson);
        var pag = ParseJson(ctx.PaginationConfigJson);
        var limits = ParseJson(ctx.RunLimitsJson);
        var chunking = ParseJson(ctx.ChunkingConfigJson);

        // Auth (API key query)
        var apiKeyParam = GetString(auth, "ApiKeyParam") ?? "api_key";
        var apiKeyValue = GetString(auth, "ApiKeyValue") ?? throw new InvalidOperationException("Auth_Config_JSON missing ApiKeyValue.");

        // Pagination config
        var pageParam = GetString(pag, "PageParam") ?? "page";
        var sizeParam = GetString(pag, "SizeParam") ?? "size";
        var startPage = GetInt(pag, "StartPage") ?? 0;

        // Limits config
        // MaxPages: >0 means cap total pages for the endpoint run; 0 means unlimited.
        var maxPages = GetInt(limits, "MaxPages") ?? 0;
        var pageSize = GetInt(limits, "PageSize") ?? 20;

        // Chunking config
        var chunkPageCount = GetInt(chunking, "ChunkPageCount") ?? 10; // default safe

        // Safety brake for unlimited mode (prevents infinite loops)
        var maxPagesHardStop = GetInt(limits, "MaxPagesHardStop") ?? 100000;

        // Folder is already pinned by RunControl and passed into ctx
        var folder = (ctx.BlobFolderPrefix ?? "").Trim();
        if (string.IsNullOrWhiteSpace(folder))
            throw new InvalidOperationException("Missing ctx.BlobFolderPrefix (should be pinned by RunControl).");

        if (!folder.EndsWith("/"))
            folder += "/";

        // Determine starting page for this chunk.
        // For capped mode (MaxPages > 0), we can compute deterministic page ranges:
        //   chunk 1 => startPage + 0
        //   chunk 2 => startPage + ChunkPageCount
        //   chunk n => startPage + (n-1)*ChunkPageCount
        // For unlimited mode, we still use this start, but we will stop early if API indicates end.
        var zeroBasedChunkIndex = Math.Max(ctx.ChunkNo - 1, 0);
        var chunkStartPage = startPage + (zeroBasedChunkIndex * chunkPageCount);

        // If capped and this chunk starts beyond the cap, we are done.
        if (maxPages > 0 && chunkStartPage >= startPage + maxPages)
        {
            return new ChunkStepResult
            {
                Success = true,
                IsDone = true,
                LastPageProcessed = chunkStartPage - 1,
                FilesWrittenThisChunk = 0,
                HttpStatusCode = 200
            };
        }

        // How many pages to attempt this chunk
        var pagesToAttempt = chunkPageCount;
        if (maxPages > 0)
        {
            var remaining = (startPage + maxPages) - chunkStartPage;
            pagesToAttempt = Math.Min(chunkPageCount, remaining);
        }

        // Blob client
        var svc = new BlobServiceClient(blobConn);
        var container = svc.GetBlobContainerClient(ctx.BlobContainer);
        await container.CreateIfNotExistsAsync(cancellationToken: ct);

        // HTTP client
        using var http = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(100)
        };

        http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        // Optional headers from config
        if (!string.IsNullOrWhiteSpace(ctx.DefaultHeadersJson))
        {
            var headers = ParseJson(ctx.DefaultHeadersJson);
            foreach (var kvp in headers.EnumerateObject())
            {
                if (kvp.Value.ValueKind == JsonValueKind.String)
                    http.DefaultRequestHeaders.TryAddWithoutValidation(kvp.Name, kvp.Value.GetString());
            }
        }

        // Accumulate JSONL lines here (streaming to blob is possible later; this is simplest first cut)
        var sb = new StringBuilder(capacity: 1024 * 1024);

        int pagesFetchedThisChunk = 0;
        int lastPageProcessed = chunkStartPage - 1;

        // Unlimited-mode stop hints
        int? totalPagesFromApi = null;

        for (var i = 0; i < pagesToAttempt; i++)
        {
            ct.ThrowIfCancellationRequested();

            // Hard stop safety (always enforced)
            if (maxPages <= 0 && pagesFetchedThisChunk >= maxPagesHardStop)
            {
                return Fail(
                    folder,
                    filesWritten: 0,
                    httpStatus: 500,
                    message: $"Stopped after MaxPagesHardStop={maxPagesHardStop}. Increase Run_Limits_JSON.MaxPagesHardStop if intended."
                );
            }

            var pageNumber = chunkStartPage + i;

            // If unlimited and we learned total_pages, stop when pageNumber reaches it
            if (maxPages <= 0 && totalPagesFromApi.HasValue && pageNumber >= totalPagesFromApi.Value)
                break;

            var url = BuildUrlWithQuery(ctx.BaseUrl, new Dictionary<string, string>
            {
                [apiKeyParam] = apiKeyValue,
                [pageParam] = pageNumber.ToString(),
                [sizeParam] = pageSize.ToString()
            });

            using var req = new HttpRequestMessage(HttpMethod.Get, url);
            using var resp = await http.SendAsync(req, ct);
            var body = await resp.Content.ReadAsStringAsync(ct);

            if (!resp.IsSuccessStatusCode)
            {
                return Fail(
                    folder,
                    filesWritten: 0,
                    httpStatus: (int)resp.StatusCode,
                    message: $"HTTP {(int)resp.StatusCode} calling {ctx.EndPointId} page={pageNumber}. Body starts: {Trunc(body, 300)}",
                    detailJson: JsonSerializer.Serialize(new { url, requestedPageSize = pageSize })
                );
            }

            // Parse NASA paging hints if unlimited
            if (maxPages <= 0)
            {
                TryReadTotalPages(body, out var totalPages);
                if (totalPages.HasValue)
                    totalPagesFromApi = totalPages.Value;
            }

            // Extract near_earth_objects array and write each element as one JSONL line
            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                if (root.TryGetProperty("near_earth_objects", out var neos) && neos.ValueKind == JsonValueKind.Array)
                {
                    foreach (var neo in neos.EnumerateArray())
                    {
                        sb.Append(neo.GetRawText());
                        sb.Append('\n');
                    }
                }
                else
                {
                    // Still write something meaningful to help diagnose bad payload shapes
                    return Fail(
                        folder,
                        filesWritten: 0,
                        httpStatus: 500,
                        message: $"Response missing $.near_earth_objects array at page={pageNumber}.",
                        detailJson: JsonSerializer.Serialize(new { url, responseStarts = Trunc(body, 300) })
                    );
                }
            }
            catch (Exception ex)
            {
                return Fail(
                    folder,
                    filesWritten: 0,
                    httpStatus: 500,
                    message: $"Failed parsing JSON at page={pageNumber}: {ex.Message}",
                    detailJson: JsonSerializer.Serialize(new { ex.GetType().FullName, ex.StackTrace })
                );
            }

            pagesFetchedThisChunk++;
            lastPageProcessed = pageNumber;
        }

        // Determine IsDone:
        // - capped mode: done when we've fetched through the last capped page
        // - unlimited mode: done if we hit total_pages boundary (or fetched fewer than requested pages)
        bool isDone;
        if (maxPages > 0)
        {
            isDone = (lastPageProcessed >= (startPage + maxPages - 1));
        }
        else
        {
            if (totalPagesFromApi.HasValue)
                isDone = (lastPageProcessed >= totalPagesFromApi.Value - 1);
            else
                isDone = (pagesFetchedThisChunk < pagesToAttempt); // if we stopped early, likely end
        }

        // If we fetched zero pages (possible at end), don't write a file
        if (pagesFetchedThisChunk <= 0)
        {
            return new ChunkStepResult
            {
                Success = true,
                IsDone = isDone,
                LastPageProcessed = lastPageProcessed,
                FilesWrittenThisChunk = 0,
                HttpStatusCode = 200
            };
        }

        // Write ONE file per chunk
        var blobName = folder + $"chunk_{ctx.ChunkNo:0000}.jsonl";
        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        await using (var ms = new MemoryStream(bytes))
        {
            await container.GetBlobClient(blobName).UploadAsync(ms, overwrite: true, cancellationToken: ct);
        }

        return new ChunkStepResult
        {
            Success = true,
            IsDone = isDone,
            LastPageProcessed = lastPageProcessed,
            FilesWrittenThisChunk = 1,
            HttpStatusCode = 200
        };
    }

    private static ChunkStepResult Fail(string folder, int filesWritten, int httpStatus, string message, string? detailJson = null)
        => new()
        {
            Success = false,
            IsDone = true,
            LastPageProcessed = null,
            FilesWrittenThisChunk = filesWritten,
            HttpStatusCode = httpStatus,
            ErrorMessage = message,
            ErrorDetailJson = detailJson
        };

    private static void TryReadTotalPages(string body, out int? totalPages)
    {
        totalPages = null;
        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;

            if (root.TryGetProperty("page", out var pageObj) &&
                pageObj.ValueKind == JsonValueKind.Object &&
                pageObj.TryGetProperty("total_pages", out var tp) &&
                tp.ValueKind == JsonValueKind.Number)
            {
                totalPages = tp.GetInt32();
            }
        }
        catch
        {
            totalPages = null;
        }
    }

    private static string BuildUrlWithQuery(string baseUrl, Dictionary<string, string> q)
    {
        var sep = baseUrl.Contains('?') ? "&" : "?";
        var sb = new StringBuilder(baseUrl);
        sb.Append(sep);

        var first = true;
        foreach (var kv in q)
        {
            if (!first) sb.Append('&');
            first = false;
            sb.Append(Uri.EscapeDataString(kv.Key));
            sb.Append('=');
            sb.Append(Uri.EscapeDataString(kv.Value));
        }
        return sb.ToString();
    }

    private static string Trunc(string s, int max) => s.Length <= max ? s : s.Substring(0, max);

    private static JsonElement ParseJson(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return JsonDocument.Parse("{}").RootElement;

        return JsonDocument.Parse(json).RootElement;
    }

    private static string? GetString(JsonElement obj, string prop)
        => obj.ValueKind == JsonValueKind.Object && obj.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString()
            : null;

    private static int? GetInt(JsonElement obj, string prop)
        => obj.ValueKind == JsonValueKind.Object && obj.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.Number
            ? v.GetInt32()
            : null;
}
