using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Catfish.Core.Models;
using Catfish.Core.Services;
using Catfish.Core.Contexts;


namespace EcopoesiaDataConverter
{
    public class ecopoesia
    {
        public string author { get; set; }
        public string titleSp { get; set; }
        public string titleEn { get; set; }
        public string contentSp { get; set; }
        public string contentEn { get; set; }
        public string category { get; set; }
        public string country { get; set; }
        public string biome { get; set; }
        public string refSp { get; set; }
        public string refEn { get; set; }
    }

    public class Author
    {
        public string fName { get; set; }
        public string lName { get; set; }
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
        public static int PoemsMetadataSetId = 1;
        public static int AuthorMetadataSetId = 2;
        public static int MAXFILES = 5000;
        public static List<Author> CreatedAuthorList = new List<Author>(); //For keep track of author who's not in the db yet but has been created in this ingestion process
        public static string AuthorGuid = string.Empty;
        public static List<Relationship> RelationshipsList = new List<Relationship>();

        static void Main(string[] args)
        {
            string currDir = Environment.CurrentDirectory;
            string pathSource = string.Empty;
            string pathOutput = string.Empty;
            string[] files;


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
                    int countFile = 0;
                    int countSaveFile = 1;
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
                        item.Add(new XAttribute("guid",itemGuid ));
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
                        Action<XElement, string, string> setValue = (field, value, valsp) =>
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
                                if(value != null)
                                   textValue.Value = value; //en

                                XElement textValue2 = new XElement("text");
                                textValue2.Add(xmlLangSp);

                                if (valsp != null)
                                {
                                    textValue2.Value = valsp;
                                   
                                }
                                valueElement.Add(textValue2);
                            }
                           
                            //textValue.Value = value ?? "";
                        };

                        Action<XElement, string> setOption = (optionsField, value) =>
                        {
                            string[] cats = value.Split(';');
                            foreach(XElement op in optionsField.Elements())
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
                            if (mEl.Element("name").Value == "Title")
                            {  
                                setValue(field, eco.titleEn, eco.titleSp);   
                            }
                            else if (mEl.Element("name").Value == "Author")
                            {   
                                setValue(field, eco.titleEn, eco.titleSp);
                            }
                            else if (mEl.Element("name").Value == "Content")
                            {
                                setValue(field, eco.contentEn, eco.contentSp);
                            }
                            else if (mEl.Element("name").Value == "Country")
                            {
                                setValue(field, eco.country, "");
                            }
                            else if (mEl.Element("name").Value == "Biome")
                            {
                                setValue(field, eco.biome, "");
                            }
                            else if (mEl.Element("name").Value == "RelatedLinks")
                            {
                                setValue(field, eco.refEn, eco.refSp);
                            }
                            else if (mEl.Element("name").Value == "Category")
                            {
                                //setValue(field, eco.country, "");
                                setOption(field.Element("options"), eco.category);
                            }

                            eFields.Add(field);
                        }

                        //create author entity
                        XElement author = createAuthorEntity(eco.author,authorMetadataSet.Id,authorMetadata, db);
                        if(author != null)
                        {
                            aggregations.Add(author);
                           
                        }

                        if (countFile == MAXFILES) //save the file for every 10k items
                        {
                            XElement _relationships = createRelationships();
                           
                            ingestion.Add(aggregations);
                            ingestion.Add(_relationships);
                            doc.Save(pathOutput + "\\EcopoedsiaIngestion-Aggregation-" + countSaveFile + ".xml");
                            countFile = 0;
                            aggregations.RemoveAll();
                            ingestion.RemoveAll();
                            countSaveFile++;

                            RelationshipsList.Clear();
                        }

                        //add relationship
                        RelationshipsList.Add(new Relationship { parentGuid = AuthorGuid, childGuid = itemGuid });
                       
                    }

                    //save file
                    XElement relationships = createRelationships();
                   
                    ingestion.Add(aggregations);
                    ingestion.Add(relationships);
                    doc.Save(pathOutput + "\\EcopoedsiaIngestion-Aggregation-" + countSaveFile + ".xml");
                   
                    aggregations.RemoveAll();
                    ingestion.RemoveAll();

                    RelationshipsList.Clear();
                   
                }
            }
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

        public static XElement createAuthorEntity(string author, int metadataSetID,XElement authorMetadata, CatfishDbContext db)
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
                    if (fName.Equals(authorNames[1].Trim(), StringComparison.InvariantCultureIgnoreCase) && lName.Equals(authorNames[1].Trim(), StringComparison.InvariantCultureIgnoreCase))
                    {
                        exist = true;
                        break;
                    }
                }

                if (!exist)//if not existing in the db, check if this author has ben created previously in the process
                {
                    if (CreatedAuthorList.Count > 0)
                    {
                        foreach (Author a in CreatedAuthorList)
                        {
                            if (a.fName.Equals(authorNames[1].Trim(), StringComparison.InvariantCultureIgnoreCase) && a.lName.Equals(authorNames[0].Trim(), StringComparison.InvariantCultureIgnoreCase))
                            {
                                exist = true;
                                break;
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
                      
                      
                        if (mEl.Element("name").Value == "FirstName")
                        {
                            setValue(field, authorNames[1].Trim(), "FirstName");
                        }
                        else if (mEl.Element("name").Value == "LastName")
                        {
                            setValue(field, authorNames[0].Trim(), "LastName");
                        }
                        

                        eFields.Add(field);
                       
                    }

                    //add this author to CreatedAuthorList<> -- so no duplication of author to be created
                    CreatedAuthorList.Add(new Author { fName = authorNames[1].Trim(), lName = authorNames[0].Trim() });

                    return item;
                }
                else
                {
                    return null;
                }
            }
            return null;
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
            using (StreamReader oReader = new StreamReader(fname, Encoding.GetEncoding("ISO-8859-1")))
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
                            foreach (XElement s in sp.Descendants("title"))
                            {
                                if (s.Name == "title")
                                {
                                    eco.titleSp = s.Value;
                                }
                            }
                            foreach (XElement s in sp.Descendants("body"))
                            {
                                if (s.Name == "body")
                                {
                                    eco.contentSp = s.Value;
                                }
                            }
                            foreach (XElement s in sp.Descendants("ref"))
                            {
                                if (s.Name == "ref")
                                {
                                    eco.refSp = s.Value;
                                }
                            }
                        }
                        foreach (XElement en in el.Descendants("en"))
                        {
                            foreach (XElement s in en.Descendants("title"))
                            {
                                if (s.Name == "title")
                                {
                                    eco.titleEn = s.Value;
                                }
                            }
                            foreach (XElement s in en.Descendants("body"))
                            {
                                if (s.Name == "body")
                                {
                                    eco.contentEn = s.Value;
                                }
                            }
                            foreach (XElement s in en.Descendants("ref"))
                            {
                                if (s.Name == "ref")
                                {
                                    eco.refEn = s.Value;
                                }
                            }
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