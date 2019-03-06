using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Catfish.Core;
using Catfish.Core.Models;
using Catfish.Core.Services;



namespace EcopoesiaDataConverter
{
    class Program
    {
        public static string EntityTypeName = "Poem Entity Type";
        public static int MetadataSetId = 170;
        static void Main(string[] args)
        {
            string currDir = Environment.CurrentDirectory;
            string pathSource = string.Empty;
            string pathOutput = string.Empty;
            string[] files;
            XDocument poemDoc = null;

            CatfishDbContext db = new CatfishDbContext();
            Catfish.Core.Contexts.AccessContext current = new AccessContext(AccessContext.PublicAccessGuid, true, db);
            AccessContext.current = current;

            if (args.Length > 0)
            {
                //1st argument is path to source files
                //2nd is the path to folder where the output will be located
                //metadataSetStructure = XDocument.Load(args[0]);
                files = Directory.GetFiles(args[0], "*.xml", SearchOption.AllDirectories);
                pathOutput = args[1];

                XDocument doc = new XDocument(new XDeclaration("1.0", "utf-8", "yes"));

                // Catfish.Core.Models.Ingestion.Ingestion ingestion = new Catfish.Core.Models.Ingestion.Ingestion();
                //  ingestion.Overwrite = false;

                if (files.Length > 0)
                {
                 

                    //if (db.Database.Connection.State == System.Data.ConnectionState.Closed)
                    //{
                    //    db.Database.Connection.Open();
                    //}

                    MetadataService mService = new MetadataService(db);
                    try
                    {
                        var metadtaSet = ( mService.GetMetadataSet(MetadataSetId));
                    }
                    catch(InvalidOperationException ex)
                    {
                        throw ex;
                    }
                    foreach (string fname in files)
                    {

                        using (StreamReader oReader = new StreamReader(fname, Encoding.GetEncoding("ISO-8859-1")))
                        {
                            poemDoc = XDocument.Load(oReader);
                            if (poemDoc.Root.Name.ToString().Equals("poem"))
                            {
                                //Catfish.Core.Models.CFMetadataSet poemMetadata = (new Catfish.Core.Services.MetadataService(db)).GetMetadataSet(MetadataSetId);
                                //AddAggregations(poemDoc, poemMetadata, pathOutput);
                            }
                        }
                    }

                }
            }
        }
    }
}
