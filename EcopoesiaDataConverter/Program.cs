using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Catfish.Core.Models;
using Catfish.Core.Services;
using Catfish.Core.Contexts;
using System.Configuration;

namespace EcopoesiaDataConverter
{
    public class LanguageContent
    {
        public string lang { get; set; }
        public string title { get; set; }
        public string content { get; set; }
        public string reference { get; set; }
    }

    public class ecopoesia
    {
        public Dictionary<string, LanguageContent> langContent { get; set; }

        public string author { get; set; }
        public string category { get; set; }
        public string country { get; set; }
        public string biome { get; set; }

        public ecopoesia()
        {
            langContent = new Dictionary<string, LanguageContent>();
            langContent.Add("en", new LanguageContent() { lang = "en" });
            langContent.Add("es", new LanguageContent() { lang = "es" });
            langContent.Add("pt", new LanguageContent() { lang = "pt" });
        }
    }

    public class Author
    {
        public string fName { get; set; }
        public string lName { get; set; }

        public List<Author> Aliases { get; set; }

        public Author()
        {
            Aliases = new List<Author>();
        }
    }

    public class Relationship
    {
        public string parentGuid { get; set; }
        public string childGuid { get; set; }
    }
    class Program
    {
        public static string PoemEntityTypeName = "Poems";
        public static string AuthorEntityTypeName = "Author";
        public static int PoemsMetadataSetId = Convert.ToInt16(ConfigurationManager.AppSettings["PoemMetadataSetId"].ToString());//170;//1;
        public static int AuthorMetadataSetId = Convert.ToInt16(ConfigurationManager.AppSettings["AuthorMetadataSetId"].ToString());//165;//2;
        public static int MAXFILES = 5000;
        public static List<Author> CreatedAuthorList = new List<Author>(); //For keep track of author who's not in the db yet but has been created in this ingestion process
        public static string AuthorGuid = string.Empty;
        public static List<Relationship> RelationshipsList = new List<Relationship>();

        static string ExtractContent(XElement body)
        {
            string line;
            string data = String.Join("", body.Nodes()).Trim();
            StringBuilder result = new StringBuilder();

            using(StringReader reader = new StringReader(data))
            {
                while((line = reader.ReadLine()) != null)
                {
                    result.Append("<p>");
                    result.Append(line.Replace("<indent", "<p").Replace("</indent>", "</p>"));
                    result.AppendLine("</p>");
                }
            }

            return result.ToString();
        }

        static void ExtractLanguageContent(LanguageContent langContent, XElement element)
        {
            foreach (XElement s in element.Descendants("title"))
            {
                if (s.Name == "title")
                {
                    langContent.title = s.Value;
                }
            }
            foreach (XElement s in element.Descendants("body"))
            {
                if (s.Name == "body")
                {
                    langContent.content = ExtractContent(s);
                }
            }
            foreach (XElement s in element.Descendants("ref"))
            {
                if (s.Name == "ref")
                {
                    langContent.reference = s.Value;
                }
            }
        }

        static void Main(string[] args)
        {
            string currDir = Environment.CurrentDirectory;
            string pathSource = string.Empty;
            string pathOutput = string.Empty;
            string[] files;
            string[] authDirs;

            CatfishDbContext db = new CatfishDbContext();
            Catfish.Core.Contexts.AccessContext current = new AccessContext(AccessContext.PublicAccessGuid, true, db);
            AccessContext.current = current;

            if (args.Length > 0)
            {
                //1st argument is path to source files
                //2nd is the path to folder where the output will be located
                //metadataSetStructure = XDocument.Load(args[0]);
                pathOutput = args[1];
                authDirs = Directory.GetDirectories(args[0],"*", SearchOption.TopDirectoryOnly);
                int countSaveFile = 1;
                for (int j=0; j < authDirs.Length - 1; j++)
                {

                    // files = Directory.GetFiles(args[0], "*.xml", SearchOption.AllDirectories);
                    files = Directory.GetFiles(authDirs[j], "*.xml", SearchOption.AllDirectories);
                    XDocument doc = new XDocument(new XDeclaration("1.0", "utf-8", "yes"));

                    // Catfish.Core.Models.Ingestion.Ingestion ingestion = new Catfish.Core.Models.Ingestion.Ingestion();
                    //  ingestion.Overwrite = false;
                    MetadataService mService = new MetadataService(db);

                    EntityService entService = new EntityService(db);

                    CFMetadataSet metadataSet = mService.GetMetadataSet(PoemsMetadataSetId);
                    CFMetadataSet authorMetadataSet = mService.GetMetadataSet(AuthorMetadataSetId);
                    XElement poemMetadata = XElement.Parse(metadataSet.Data.ToString());
                    XElement authorMetadata = XElement.Parse(authorMetadataSet.Data.ToString());


                    XElement ingestion = new XElement("ingestion");
                    ingestion.Add(new XAttribute("overwrite", "false"));
                    doc.Add(ingestion);

                    XAttribute xmlLangEn = new XAttribute(XNamespace.Xml + "lang", "en");
                    XElement aggregations = new XElement("aggregations");
                    //int fileCount = 1;
                    XAttribute xmlLangSp = new XAttribute(XNamespace.Xml + "lang", "es");
                    XAttribute xmlLangPt = new XAttribute(XNamespace.Xml + "lang", "pt");
                    int countFile = 0;
                   

                    if (files.Length > 0)
                    {
                        

                        //find author aliases
                        Author _authorName = new Author();
                        foreach (string f in files) //each file = each item
                        {
                            getAuthorAliases(f, _authorName);
                        }

                            foreach (string fname in files) //each file = each item
                        {
                            countFile++;
                            XElement item = new XElement("item");
                            aggregations.Add(item);
                            string now = DateTime.Now.ToShortDateString();
                            item.Add(new XAttribute("created", now));
                            item.Add(new XAttribute("updated", now));
                            item.Add(new XAttribute("model-type", "Catfish.Core.Models.CFItem, Catfish.Core, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));
                            item.Add(new XAttribute("IsRequired", "false"));
                            string itemGuid = Guid.NewGuid().ToString();
                            item.Add(new XAttribute("guid", itemGuid));
                            item.Add(new XAttribute("entity-type", PoemEntityTypeName));

                            XElement metadata = new XElement("metadata");
                            item.Add(metadata);
                            XElement mdSet = new XElement("metadata-set");
                            foreach (XAttribute att in poemMetadata.Attributes()) //grab the metadataset and all it's attribute
                            {
                                mdSet.Add(new XAttribute(att.Name, att.Value));
                            }

                            metadata.Add(mdSet);
                            XElement eFields = new XElement("fields");
                            mdSet.Add(eFields);

                            XElement fields = poemMetadata.Element("fields");

                            ecopoesia eco = getContent(fname); //grab all the content from input file

                            //set text value
                            Action<XElement, string, string, string> setValue = (field, value, valsp, valpt) =>
                            {
                                XElement valueElement = field.Element("value");

                                if (valueElement == null)
                                {
                                    valueElement = new XElement("value");
                                    field.Add(valueElement);
                                }

                                XElement textValue = valueElement.Element("text");

                                if (textValue == null)
                                {
                                    textValue = new XElement("text");
                                    valueElement.Add(textValue);

                                    textValue.Add(xmlLangEn);
                                    if (value != null)
                                        textValue.Value = value; //en

                                XElement textValue2 = new XElement("text");
                                    textValue2.Add(xmlLangSp);

                                    if (valsp != null)
                                    {
                                        textValue2.Value = valsp;

                                    }
                                    valueElement.Add(textValue2);

                                    XElement textValue3 = new XElement("text");
                                    textValue3.Add(xmlLangPt);

                                    if (valpt != null)
                                    {
                                        textValue3.Value = valpt;

                                    }
                                    valueElement.Add(textValue3);
                                }
                            };

                            Action<XElement, string> setOption = (optionsField, value) =>
                            {
                                string[] cats = value.Split(';');
                                foreach (XElement op in optionsField.Elements())
                                {
                                    for (int i = 0; i < cats.Length; i++)
                                    {
                                        if (op.Value.Equals(cats[i].Trim(), StringComparison.OrdinalIgnoreCase))
                                        {
                                            op.SetAttributeValue("selected", true);
                                            break;
                                        }
                                    }
                                }
                            };


                            foreach (XElement mEl in poemMetadata.Descendants("field"))
                            {
                                XElement field = new XElement(mEl);
                                string fieldName = mEl.Element("name").Elements().First().Value;

                                if (fieldName == "Title")
                                {
                                    setValue(field, eco.langContent["en"].title, eco.langContent["es"].title, eco.langContent["pt"].title);
                                }
                                else if (fieldName == "Author")
                                {
                                    setValue(field, eco.langContent["en"].title, eco.langContent["es"].title, eco.langContent["pt"].title);
                                }
                                else if (fieldName == "Content")
                                {
                                    setValue(field, eco.langContent["en"].content, eco.langContent["es"].content, eco.langContent["pt"].content);
                                }
                                else if (fieldName == "Country")
                                {
                                    setValue(field, eco.country, "", "");
                                }
                                else if (fieldName == "Biome")
                                {
                                    setValue(field, eco.biome, "", "");
                                }
                                else if (fieldName == "RelatedLinks")
                                {
                                    setValue(field, eco.langContent["en"].reference, eco.langContent["es"].reference, eco.langContent["pt"].reference);
                                }
                                else if (fieldName == "Category")
                                {
                                    //setValue(field, eco.country, "");
                                    setOption(field.Element("options"), eco.category);
                                }

                                eFields.Add(field);
                            }

                            //create author entity
                            XElement author = createAuthorEntity(eco.author, authorMetadataSet.Id, authorMetadata, db, _authorName);
                            if (author != null)
                            {
                                aggregations.Add(author);

                            }

                            //if (countFile == MAXFILES) //save the file for every 10k items
                            //{
                            //    XElement _relationships = createRelationships();

                            //    ingestion.Add(aggregations);
                            //    ingestion.Add(_relationships);
                            //    doc.Save(pathOutput + "\\EcopoedsiaIngestion-Aggregation-" + countSaveFile + ".xml");
                            //    countFile = 0;
                            //    aggregations.RemoveAll();
                            //    ingestion.RemoveAll();
                            //    countSaveFile++;

                            //    RelationshipsList.Clear();
                            //}

                            //add relationship
                            RelationshipsList.Add(new Relationship { parentGuid = AuthorGuid, childGuid = itemGuid });

                        }

                        //save file

                        //if (countFile == MAXFILES) //save the file for every 10k items
                        //{
                        //    XElement _relationships = createRelationships();

                        //    ingestion.Add(aggregations);
                        //    ingestion.Add(_relationships);
                        //    doc.Save(pathOutput + "\\EcopoedsiaIngestion-Aggregation-" + countSaveFile + ".xml");
                        //    countFile = 0;
                        //    aggregations.RemoveAll();
                        //    ingestion.RemoveAll();
                        //    countSaveFile++;

                        //    RelationshipsList.Clear();
                        //}
                        XElement relationships = createRelationships();

                        ingestion.Add(aggregations);
                        ingestion.Add(relationships);

                        WriteDocument(pathOutput + "\\EcopoedsiaIngestion-Aggregation-" + countSaveFile + ".xml", doc);
                        countSaveFile++;
                        aggregations.RemoveAll();
                        ingestion.RemoveAll();

                        RelationshipsList.Clear();

                    }
                }
            }
        }

        public static Author getAuthorAliases(string fname, Author author)
        {

            XDocument poemDoc = null;
            Encoding encoding = GetEncoding(fname);



            using (StreamReader oReader = new StreamReader(fname, encoding))
            {
                poemDoc = XDocument.Load(oReader);
                if (poemDoc.Root.Name.ToString().Equals("poem"))
                {
                    //author
                    foreach (XElement el in poemDoc.Descendants("author"))
                    {
                        if (el.Name == "author")
                        {
                            string name = el.Value;
                            if (!string.IsNullOrEmpty(name))
                            {
                                string[] authorNames = name.Split(';'); //1st - last name, 2nd first name
                                if (string.IsNullOrEmpty(author.lName) && string.IsNullOrEmpty(author.fName))
                                {
                                    author.fName = authorNames[1].Trim();
                                    author.lName = authorNames[0].Trim();
                                }
                                else
                                {
                                    //check aliases
                                    if (!(author.fName.Equals(authorNames[1].Trim())) || !(author.lName.Equals(authorNames[0].Trim())))
                                    {
                                        if (author.Aliases.Count > 0)
                                        {
                                            foreach (Author a in author.Aliases)
                                            {
                                                if (!(a.fName.Equals(authorNames[1].Trim())) || !(a.lName.Equals(authorNames[0].Trim())))
                                                {
                                                    author.Aliases.Add(new Author { fName = authorNames[1].Trim(), lName = authorNames[0].Trim() });
                                                }
                                                else { break; }
                                            }
                                        }
                                        else
                                        {
                                            author.Aliases.Add(new Author { fName = authorNames[1].Trim(), lName = authorNames[0].Trim() });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        

            return author;
        }

        public static XElement createRelationships()
        {
            XElement Relationships = new XElement("relationships");
           
            foreach(Relationship r in RelationshipsList)
            {
                XElement relationship = new XElement("relationship");
                Relationships.Add(relationship);
                string now = DateTime.Now.ToShortDateString();
                relationship.Add(new XAttribute("created", now));
                relationship.Add(new XAttribute("updated", now));
                relationship.Add(new XAttribute("model-type", "Catfish.Core.Models.Ingestion.Relationship, Catfish.Core, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));
                relationship.Add(new XAttribute("overwrite", "false"));
                string relGuid = Guid.NewGuid().ToString();
                relationship.Add(new XAttribute("guid", relGuid));

                XElement parent = new XElement("parent");
                parent.Add(new XAttribute("ref", r.parentGuid));
                XElement child = new XElement("child");
                child.Add(new XAttribute("ref", r.childGuid));

                relationship.Add(parent);
                relationship.Add(child);
            }

            return Relationships;
        }

        public static XElement createAuthorEntity(string author, int metadataSetID,XElement authorMetadata, CatfishDbContext db, Author aliasName= null)
        {
            if(!string.IsNullOrEmpty(author))
            {
                string[] authorNames = author.Split(';'); //1st - last name, 2nd first name
                EntityService entService = new EntityService(db);
                var authors = entService.GetEntitiesWithMetadataSet(metadataSetID);
                bool exist = false;
                foreach(CFEntity au in authors)
                {
                    XElement authorElement = XElement.Parse(au.Data.ToString());
                    string fName = string.Empty, lName = string.Empty;
                    foreach (XElement mEl in authorElement.Descendants("field"))
                    {
                        
                        if (mEl.Element("name").Value == "FirstName")
                        {
                            fName = mEl.Value.Substring(9).Trim();
                        }
                        if (mEl.Element("name").Value == "LastName")
                        {
                            lName = mEl.Value.Substring(8).Trim();
                        }
                    }

                    //check if author has been created in the db
                    if(lName.Equals(authorNames[0].Trim(), StringComparison.InvariantCultureIgnoreCase))
                    {
                        if(authorNames.Length == 1)
                        {
                            exist = string.IsNullOrEmpty(fName);
                        }
                        else
                        {
                            exist = fName.Equals(authorNames[1].Trim(), StringComparison.InvariantCultureIgnoreCase);
                        }

                        if (exist)
                        {
                            break;
                        }
                    }
                }

                if (!exist)//if not existing in the db, check if this author has ben created previously in the process
                {
                    if (CreatedAuthorList.Count > 0)
                    {
                        foreach (Author a in CreatedAuthorList)
                        {
                            if (a.lName.Equals(authorNames[0].Trim(), StringComparison.InvariantCultureIgnoreCase))
                            {
                                if (authorNames.Length == 1)
                                {
                                    exist = string.IsNullOrEmpty(a.fName);
                                }
                                else
                                {
                                    exist = a.fName.Equals(authorNames[1].Trim(), StringComparison.InvariantCultureIgnoreCase);
                                }

                                if (exist)
                                {
                                    
                                    break;
                                }
                            }
                        }
                    }
                }
                if (!exist)
                {
                    XElement item = new XElement("item");
                    XAttribute xmlLangEn = new XAttribute(XNamespace.Xml + "lang", "en");

                    // aggregations.Add(item);
                    string now = DateTime.Now.ToShortDateString();
                    item.Add(new XAttribute("created", now));
                    item.Add(new XAttribute("updated", now));
                    item.Add(new XAttribute("model-type", "Catfish.Core.Models.CFItem, Catfish.Core, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));
                    item.Add(new XAttribute("IsRequired", "false"));
                    AuthorGuid = Guid.NewGuid().ToString();
                    item.Add(new XAttribute("guid", AuthorGuid));
                    item.Add(new XAttribute("entity-type", AuthorEntityTypeName));

                    XElement metadata = new XElement("metadata");
                    item.Add(metadata);
                    XElement mdSet = new XElement("metadata-set");
                    foreach (XAttribute att in authorMetadata.Attributes()) //grab the metadataset and all it's attribute
                    {
                        mdSet.Add(new XAttribute(att.Name, att.Value));
                    }

                    metadata.Add(mdSet);
                    XElement eFields = new XElement("fields");
                    mdSet.Add(eFields);

                    XElement fields = authorMetadata.Element("fields");

                    //set text value
                    Action<XElement, string, string> setValue = (field, value, fieldName) =>
                    {
                        XElement valueElement = field.Element("value");

                        if (valueElement == null)
                        {
                            valueElement = new XElement("value");
                            field.Add(valueElement);
                        }

                        XElement textValue = valueElement.Element("text");

                        if (textValue == null )
                        {
                            textValue = new XElement("text");
                            valueElement.Add(textValue);

                            textValue.Add(xmlLangEn);
                            textValue.Value = value; //en
                            
                        }
                       
                    };

                    foreach (XElement mEl in authorMetadata.Descendants("field"))
                    {
                        XElement field =new XElement(mEl);
                      
                      
                        if (mEl.Element("name").Value == "FirstName" && authorNames.Length > 1)
                        {
                            setValue(field, authorNames[1].Trim(), "FirstName");
                            
                        }
                        else if (mEl.Element("name").Value == "LastName")
                        {
                            setValue(field, authorNames[0].Trim(), "LastName");
                        }
                        
                        if(aliasName.Aliases.Count > 0)
                        {
                            foreach(Author a in aliasName.Aliases)
                            {
                                setValue(field, a.fName, "FirstName");
                                setValue(field, a.lName, "LastName");
                            }

                            aliasName.Aliases.Clear(); //add only once
                        }

                        eFields.Add(field);
                       
                    }

                    //add this author to CreatedAuthorList<> -- so no duplication of author to be created
                    CreatedAuthorList.Add(new Author { fName = authorNames.Length > 1 ? authorNames[1].Trim() : string.Empty, lName = authorNames[0].Trim() });

                    return item;
                }
                else
                {
                    return null;
                }
            }
            return null;
        }

        public static void WriteDocument(string path, XDocument doc)
        {
            using(StreamWriter writer = new StreamWriter(path, false, Encoding.UTF8))
            {
                doc.Save(writer);
            }
        }

        // Code Found HERE: https://stackoverflow.com/questions/3825390/effective-way-to-find-any-files-encoding
        // And HERE: https://social.msdn.microsoft.com/Forums/en-US/b172cd4d-25fe-4696-8c0f-37226c053d71/how-to-detect-encoding-file-in-ansi-utf8-and-utf8-without-bom?forum=csharpgeneral
        // This was modified to check if we had a non BOM UTF-8 file.
        public static Encoding GetEncoding(string filename)
        {
            // Read the BOM
            var bom = new byte[4];
            
            using (var file = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                file.Read(bom, 0, 4);
            }

            // Analyze the BOM
            if (bom[0] == 0x2b && bom[1] == 0x2f && bom[2] == 0x76) return Encoding.UTF7;
            if (bom[0] == 0xef && bom[1] == 0xbb && bom[2] == 0xbf) return Encoding.UTF8;
            if (bom[0] == 0xff && bom[1] == 0xfe) return Encoding.Unicode; //UTF-16LE
            if (bom[0] == 0xfe && bom[1] == 0xff) return Encoding.BigEndianUnicode; //UTF-16BE
            if (bom[0] == 0 && bom[1] == 0 && bom[2] == 0xfe && bom[3] == 0xff) return Encoding.UTF32;
            if (ValidateUtf8NoBOM(filename)) return Encoding.UTF8;

            return Encoding.Default;
        }

        private static bool ValidateUtf8NoBOM(string FileSource)
        {
            bool bReturn = false;
            string TextANSI = "";

            //Read the file as  ANSI
            using (StreamReader reader = new StreamReader(FileSource, Encoding.Default, false)) {
                TextANSI = reader.ReadToEnd();
            }

            // if the file contains special characters is UTF8 text read ansi show signs

            if (TextANSI.Contains("Ã") || TextANSI.Contains("±"))
                 bReturn = true;

            return bReturn;
        }

        /// <summary>
        /// extract content from xml input file
        /// </summary>
        /// <param name="fname"></param>
        /// <returns></returns>
        public static ecopoesia getContent(string fname) 
        {
            ecopoesia eco = new ecopoesia();
            XDocument poemDoc = null;
            Encoding encoding = GetEncoding(fname);

            Console.WriteLine("{0} {1}", fname, encoding.EncodingName);

            using (StreamReader oReader = new StreamReader(fname, encoding))
            {
                poemDoc = XDocument.Load(oReader);
                if (poemDoc.Root.Name.ToString().Equals("poem"))
                {
                    //author
                    foreach (XElement el in poemDoc.Descendants("author"))
                    {
                        if (el.Name == "author")
                        {
                            eco.author = el.Value;
                        }

                    }
                    foreach (XElement el in poemDoc.Descendants("text"))
                    {
                        foreach (XElement sp in el.Descendants("sp"))
                        {
                            ExtractLanguageContent(eco.langContent["es"], sp);
                        }
                        foreach (XElement en in el.Descendants("en"))
                        {
                            ExtractLanguageContent(eco.langContent["en"], en);
                        }
                        foreach (XElement en in el.Descendants("p"))
                        {
                            ExtractLanguageContent(eco.langContent["pt"], en);
                        }
                    }
                    foreach (XElement el in poemDoc.Descendants("category"))
                    {
                        if (el.Name == "category")
                        {
                            eco.category = el.Value;
                        }

                    }
                    foreach (XElement el in poemDoc.Descendants("biome"))
                    {
                        if (el.Name == "biome")
                        {
                            eco.biome = el.Value;
                        }

                    }
                    foreach (XElement el in poemDoc.Descendants("country"))
                    {
                        if (el.Name == "country")
                        {
                            eco.country = el.Value;
                        }

                    }
                }
            }

            return eco;
        }
    }



}