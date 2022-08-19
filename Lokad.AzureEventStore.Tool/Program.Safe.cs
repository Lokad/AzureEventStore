using System;
using System.Text.RegularExpressions;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;

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

                var c = new StorageSharedKeyCredential(account, key);
                var u = new Uri($"https://{account}.blob.core.windows.net");
                var ca = new BlobServiceClient(u, c);

                var sas = ca.GenerateAccountSasUri(
                    permissions: AccountSasPermissions.List | AccountSasPermissions.Read,
                    expiresOn: DateTimeOffset.UtcNow + TimeSpan.FromDays(365),
                    resourceTypes:
                        AccountSasResourceTypes.Container | AccountSasResourceTypes.Object |
                        AccountSasResourceTypes.Service
                );

                var signed = "BlobEndpoint=" + ca.Uri + ";SharedAccessSignature=" + sas;

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