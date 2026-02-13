using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace Ingestion.Functions;

public sealed class A_IngestRun_Control_snbx2 {
    private readonly ILogger _log;
    private readonly IApiConfigRepository_snbx _repo;
    private readonly IRunControlRepository _runRepo;
    private readonly IChunkProcessorRegistry _chunkRegistry;

    public A_IngestRun_Control_snbx2(
        ILoggerFactory loggerFactory,
        IApiConfigRepository_snbx repo,
        IRunControlRepository runRepo,
        IChunkProcessorRegistry chunkRegistry)
    {
        _log = loggerFactory.CreateLogger<A_IngestRun_Control_snbx2>();
        _repo = repo;
        _runRepo = runRepo;
        _chunkRegistry = chunkRegistry;
    }

    [Function(nameof(A_IngestRun_Control_snbx2))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "ingest/run_snbx2")] HttpRequestData req,
        CancellationToken ct)
    {
        var res = req.CreateResponse();
        IngestRunRequestSnbx2? input;

        try
        {
            input = await JsonSerializer.DeserializeAsync<IngestRunRequestSnbx2>(req.Body, JsonOpts, ct);
        }
        catch
        {
            res.StatusCode = HttpStatusCode.BadRequest;
            await res.WriteStringAsync("Invalid JSON. Expect {\"Client_ID\":1,\"EndPoint_ID\":\"NEO_BROWSE_SNBX2\",\"RunId\":\"<guid>\",\"chunk_no\":1}", ct);
            return res;
        }

        if (input is null || input.Client_ID <= 0 || string.IsNullOrWhiteSpace(input.EndPoint_ID))
        {
            res.StatusCode = HttpStatusCode.BadRequest;
            await res.WriteStringAsync("Missing required fields: Client_ID, EndPoint_ID.", ct);
            return res;
        }

        if (string.IsNullOrWhiteSpace(input.RunId))
        {
            res.StatusCode = HttpStatusCode.BadRequest;
            await res.WriteStringAsync("Missing required field: RunId (GUID). ADF must generate RunId and pass it.", ct);
            return res;
        }

        if (!Guid.TryParse(input.RunId, out var runGuid))
        {
            res.StatusCode = HttpStatusCode.BadRequest;
            await res.WriteStringAsync("RunId must be a valid GUID.", ct);
            return res;
        }

        if (input.chunk_no <= 0)
        {
            res.StatusCode = HttpStatusCode.BadRequest;
            await res.WriteStringAsync("Missing/invalid required field: chunk_no (must be >= 1).", ct);
            return res;
        }

        _log.LogInformation("SNBX2 Run start. Client_ID={ClientId}, EndPoint_ID={EndPointId}, RunId={RunId}, chunk_no={ChunkNo}",
            input.Client_ID, input.EndPoint_ID, input.RunId, input.chunk_no);

        // 1) Load API config
        ApiConfigRowSnbx? cfg;
        try
        {
            cfg = await _repo.GetConfigAsync(input.Client_ID, input.EndPoint_ID, ct);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "SQL config lookup failed.");
            res.StatusCode = HttpStatusCode.InternalServerError;
            await res.WriteStringAsync("SQL config lookup failed.", ct);
            return res;
        }

        if (cfg is null)
        {
            res.StatusCode = HttpStatusCode.NotFound;
            await res.WriteStringAsync("No active config found for that Client_ID + EndPoint_ID.", ct);
            return res;
        }

        // 2) Create/get RunControl (parent) and pin folder prefix
        RunControlInfo runInfo;
        try
        {
            if (input.chunk_no == 1)
            {
                runInfo = await _runRepo.CreateRunAsync(
                    runGuid: runGuid,
                    runIdText: input.RunId!,
                    clientId: cfg.Client_ID,
                    endPointId: cfg.EndPoint_ID,
                    blobContainer: cfg.Blob_Container_Name,
                    blobPathPrefix: cfg.Blob_Path_Prefix,
                    runLimitsJson: cfg.Run_Limits_JSON,
                    chunkingConfigJson: cfg.Chunking_Config_JSON,
                    ct: ct);
            }
            else
            {
                runInfo = await _runRepo.GetRunAsync(runGuid, ct);
            }
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "RunControl init/get failed.");
            res.StatusCode = HttpStatusCode.InternalServerError;
            await res.WriteStringAsync("RunControl init/get failed.", ct);
            return res;
        }

        // 3) Resolve chunk processor
        var processor = _chunkRegistry.Resolve(cfg.Processor_Function);
        if (processor is null)
        {
            res.StatusCode = HttpStatusCode.InternalServerError;
            await res.WriteStringAsync($"Chunk processor not registered: {cfg.Processor_Function}", ct);
            return res;
        }

        // 4) Mark chunk start (writes child chunk row + sets parent InProgress markers)
        try
        {
            await _runRepo.MarkChunkStartAsync(runGuid, input.chunk_no, ct);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "RunControl MarkChunkStart failed.");
            res.StatusCode = HttpStatusCode.InternalServerError;
            await res.WriteStringAsync("RunControl MarkChunkStart failed.", ct);
            return res;
        }

        // 5) Execute chunk processor
        ChunkStepResult step;
        ChunkIngestContext ctx;

        try
        {
            ctx = new ChunkIngestContext
            {
                RunGuid = runGuid,
                RunIdText = input.RunId!,
                ChunkNo = input.chunk_no,

                ClientId = cfg.Client_ID,
                EndPointId = cfg.EndPoint_ID,
                Vendor = cfg.API_Vendor_Name,

                BlobContainer = cfg.Blob_Container_Name,
                BlobPathPrefix = cfg.Blob_Path_Prefix,
                BlobFileNamingJson = cfg.Blob_File_Naming_JSON,

                BaseUrl = cfg.Base_URL,
                HttpMethod = cfg.Http_Method,

                DefaultHeadersJson = cfg.Default_Headers_JSON,
                AuthType = cfg.Auth_Type,
                AuthConfigJson = cfg.Auth_Config_JSON,

                PaginationType = cfg.Pagination_Type,
                PaginationConfigJson = cfg.Pagination_Config_JSON,

                RunLimitsJson = cfg.Run_Limits_JSON,
                ChunkingConfigJson = cfg.Chunking_Config_JSON,
                ProcessorConfigJson = cfg.Processor_Config_JSON,

                // pinned from parent
                BlobFolderPrefix = runInfo.BlobFolderPrefix
            };

            step = await processor.ExecuteAsync(ctx, ct);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Chunk processor execution threw.");
            try
            {
                await _runRepo.MarkChunkEndAsync(
                    runGuid: runGuid,
                    chunkNo: input.chunk_no,
                    success: false,
                    startPage: null,
                    endPage: null,
                    filesWritten: 0,
                    httpStatusCode: 500,
                    errorMessage: ex.Message,
                    errorDetailJson: JsonSerializer.Serialize(new { ex.GetType().FullName, ex.StackTrace }),
                    ct: ct);
            }
            catch
            {
                // swallow secondary failures
            }

            res.StatusCode = HttpStatusCode.InternalServerError;
            await res.WriteStringAsync($"Chunk processor execution failed: {ex.Message}", ct);
            return res;
        }

        // 6) Mark chunk end + progress (writes chunk row + rolls up to parent)
        try
        {
            await _runRepo.MarkChunkEndAsync(
                runGuid: runGuid,
                chunkNo: input.chunk_no,
                success: step.Success,
                startPage: null,
                endPage: step.LastPageProcessed,
                filesWritten: step.FilesWrittenThisChunk ?? 0,
                httpStatusCode: step.HttpStatusCode,
                errorMessage: step.ErrorMessage,
                errorDetailJson: step.ErrorDetailJson,
                ct: ct);

            if (step.Success)
            {
                await _runRepo.UpdateProgressAsync(
                    runGuid: runGuid,
                    chunkNoCompleted: input.chunk_no,
                    lastPageProcessed: step.LastPageProcessed,
                    filesWrittenIncrement: step.FilesWrittenThisChunk ?? 0,
                    httpStatusCode: step.HttpStatusCode,
                    ct: ct);
            }

            if (step.Success && step.IsDone)
            {
                await _runRepo.MarkCompletedAsync(runGuid, ct);
            }

            if (!step.Success)
            {
                await _runRepo.MarkFailedAsync(
                    runGuid: runGuid,
                    httpStatusCode: step.HttpStatusCode ?? 500,
                    errorMessage: step.ErrorMessage ?? "Chunk failed.",
                    errorDetailJson: step.ErrorDetailJson,
                    ct: ct);
            }
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "RunControl update failed after chunk execution.");
            res.StatusCode = HttpStatusCode.InternalServerError;
            await res.WriteStringAsync("RunControl update failed after chunk execution.", ct);
            return res;
        }

        // 7) Return output to ADF
        var output = new MasterOutputSnbx2
        {
            Success = step.Success,
            IsDone = step.IsDone,
            RunId = input.RunId!,

            Client_ID = cfg.Client_ID,
            EndPoint_ID = cfg.EndPoint_ID,

            Client_Name = cfg.Client_Name,
            API_Vendor_Name = cfg.API_Vendor_Name,
            EndPoint_Name = cfg.EndPoint_Name,

            Base_URL = cfg.Base_URL,
            Http_Method = cfg.Http_Method,

            Processor_Function = cfg.Processor_Function,

            Blob_Container_Name = cfg.Blob_Container_Name,
            BlobFolderPrefix = runInfo.BlobFolderPrefix,
            FilesWritten = step.FilesWrittenThisChunk,

            Message = step.Success
                ? (step.IsDone ? "Chunk completed; run done." : "Chunk completed; more chunks pending.")
                : (step.ErrorMessage ?? "Chunk failed.")
        };

        res.StatusCode = output.Success ? HttpStatusCode.OK : HttpStatusCode.InternalServerError;
        await res.WriteAsJsonAsync(output, cancellationToken: ct);
        return res;
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true
    };
}

public sealed class IngestRunRequestSnbx2 {
    public int Client_ID { get; set; }
    public string EndPoint_ID { get; set; } = "";
    public string? RunId { get; set; }
    public int chunk_no { get; set; }
}

public sealed class MasterOutputSnbx2
{
    public bool Success { get; set; }
    public bool IsDone { get; set; }

    public string RunId { get; set; } = "";

    public int Client_ID { get; set; }
    public string EndPoint_ID { get; set; } = "";

    public string Client_Name { get; set; } = "";
    public string API_Vendor_Name { get; set; } = "";
    public string EndPoint_Name { get; set; } = "";

    public string Base_URL { get; set; } = "";
    public string Http_Method { get; set; } = "";

    public string Processor_Function { get; set; } = "";

    public string Blob_Container_Name { get; set; } = "";
    public string? BlobFolderPrefix { get; set; }
    public int? FilesWritten { get; set; }

    public string? Message { get; set; } }

