using System.ServiceModel;
using System.ServiceModel.Channels;

using Ukg.DataService;
using Ukg.StreamService;

namespace Ukg.Pro.Raas;

// https://learningcenter.ultimatesoftware.com/a/207076

public static class ReportRetriever
{
    /// <summary>
    /// Retrieves a report via the UKG RaaS API.
    /// </summary>
    /// <exception cref="UnauthorizedAccessException">Returned if logon to UKG BI Data Service fails.</exception>
    public static async void GetReport()
    {
        DataContext dataContext;
        HttpRequestMessageProperty httpHeader = new();
        httpHeader.Headers["US-DELIMITER"] = ","; // A header that requests CSV format instead of XML.
        ReportResponse reportResponse;

        // If you're authenticating with a token, fill in this value and the customerApiKey:
        // const string token = "";
        // If not, you're authenticating normally, so fill in these values:
        const string username = "";
        const string password = "";
        const string userApiKey = "";
        const string customerApiKey = "";

        // If you're using the Cognos BI report ID, fill in this value:
        // const string reportId = "";

        // If not, you're using the Cognos BI report path, so fill in this value:
        const string reportPath = @"";

        // Choose this reportRequest if sending the Cognos BI report ID:
        // ReportRequest reportRequest = new() { ReportPath = reportId };

        // If not, you're using the Cognos BI report path, so choose this one:
        ReportRequest reportRequest = new() { ReportPath = reportPath };

        // If you're using a token, use this request:
        //LogOnWithTokenRequest tokenRequest = new()
        //{
        //    Token = token,
        //    ClientAccessKey = customerApiKey
        //};

        // If not, you're authenticating normally, so you'll use this tokenRequest:
        LogOnRequest logonRequest = new()
        {
            ClientAccessKey = customerApiKey,
            UserAccessKey = userApiKey,
            UserName = username,
            Password = password
        };

        using BIDataServiceClient dataClient = new(BIDataServiceClient.EndpointConfiguration.WSHttpBinding_IBIDataService);

        // Logon to the UKG BI Data Service:
        try
        {
            // Token authentication:
            // dataContext = await dataClient.LogOnWithTokenAsync(tokenRequest);
            // Normal authentication:
            dataContext = await dataClient.LogOnAsync(logonRequest);

            if (dataContext.Status != ContextStatus.Ok)
                throw new UnauthorizedAccessException(dataContext.StatusMessage);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to log in to UKG BI Data Service: {ex.Message}");
            throw;
        }

        Console.WriteLine("Successfully logged on to UKG BI Data Service.");
        
        // Authentication succeeded; get a context:
        using (new OperationContextScope(dataClient.InnerChannel))
        {
            // Add the header to the outgoing message:
            OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] = httpHeader;

            // Optional: If you need report parameters:
            /*            
            try
            {
                Console.WriteLine("Sending request for report parameters to UKG BI Data Service...");
                ReportParameterResponse paramResponse = await dataClient.GetReportParametersAsync(reportPath, dataContext);
                if (paramResponse.Status != ReportRequestStatus.Success)
                    throw new Exception(paramResponse.StatusMessage);
                else
                    foreach (ReportParameter param in paramResponse.ReportParameters)
                        Console.WriteLine($"name={param.Name}, value={param.Value}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to retrieve report parameters from UKG BI Data Service: {ex.Message}");
                throw;
            }
            */

            Console.WriteLine("Sending report request to UKG BI Data Service...");
            try
            {
                // Send the request and save its reportResponse:
                reportResponse = await dataClient.ExecuteReportAsync(reportRequest, dataContext);
                if (reportResponse.Status != ReportRequestStatus.Success)
                    throw new Exception(reportResponse.StatusMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed report request to UKG BI Data Service: {ex.Message}");
                throw;
            }
            try
            {
                Console.WriteLine("Successfully retrieved report response.  Streaming report to file...");
                // Stream the contents of the report into a file:
                await GetReportStreamFromResponseAsync(reportResponse);
                Console.WriteLine("Successfully streamed report to file.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to stream report to file: {ex.Message}");
                throw;
            }
            finally
            {
                // Log off the BI Data Service whether streaming the report to the file succeeds or not:
                await dataClient.LogOffAsync(dataContext);
            }
        }
    }
    /// <summary>
    /// Given a ReportResponse from the UKG BI Data Service, stream it to a file.
    /// </summary>
    /// <param name="reportResponse">The ReportResponse received via GetReport().</param>
    /// <returns>Task</returns>
    /// <exception cref="Exception">Thrown if retrieving the report stream fails.</exception>
    private static async Task GetReportStreamFromResponseAsync(ReportResponse reportResponse)
    {
        // Set this to the path where you want the report saved:
        const string destination = "./data/StreamOutput.csv";

        // Create the stream client using the ReportRetrievalUri provided by the ReportResponse object:
        using BIStreamServiceClient streamClient = new(
                BIStreamServiceClient.EndpointConfiguration.WSHttpBinding_IBIStreamService,
                new EndpointAddress(reportResponse.ReportRetrievalUri)
        );

        // Stream the report into a StreamReportResponse:
        StreamReportResponse streamReportResponse = await streamClient.RetrieveReportAsync(reportResponse.ReportKey);

        if (streamReportResponse.Status == ReportResponseStatus.Failed)
            throw new Exception($"Failed to stream the BI report: {streamReportResponse.StatusMessage}.");

        // Build a reader, writer and buffer:
        using StreamReader reader = new(streamReportResponse.ReportStream);
        using StreamWriter writer = new(destination);
        char[] buffer = new char[1024];

        // Read StreamReportResponse into the buffer:
        while (reader.Peek() > -1)
            await reader.ReadAsync(buffer);

        // Write the buffer to the destination:
        await writer.WriteAsync(buffer);
    }
}
