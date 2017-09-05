using System;
using System.Text.RegularExpressions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;

namespace Lokad.AzureEventStore.Tool
{
    static partial class Program
    {
        private static void CmdSafe(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: safe <stream>");
                Console.WriteLine("Prints a read-only connection token to the stream.");
                return;
            }

            try
            {
                var arg = Parse(args[0]);
                var account = new Regex("AccountName=([^;]*)").Match(arg).Groups[1].Value;
                var key = new Regex("AccountKey=([^;]*)").Match(arg).Groups[1].Value;

                var c = new StorageCredentials(account, key);
                var ca = new CloudStorageAccount(c, true);

                var sas = ca.GetSharedAccessSignature(new SharedAccessAccountPolicy
                {
                    Permissions = SharedAccessAccountPermissions.List | SharedAccessAccountPermissions.Read,
                    Protocols = SharedAccessProtocol.HttpsOnly,
                    SharedAccessExpiryTime = DateTimeOffset.UtcNow + TimeSpan.FromDays(365),
                    Services = SharedAccessAccountServices.Blob,
                    ResourceTypes =
                        SharedAccessAccountResourceTypes.Container | SharedAccessAccountResourceTypes.Object |
                        SharedAccessAccountResourceTypes.Service,
                    SharedAccessStartTime = DateTimeOffset.UtcNow
                });

                var signed = "BlobEndpoint=" + ca.BlobStorageUri.PrimaryUri + ";SharedAccessSignature=" + sas;

                var container = new Regex("Container=[^;]*").Match(arg);
                if (container.Success)
                    signed += ";" + container.Value;

                Console.WriteLine("{0}", signed);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
        }
    }
}