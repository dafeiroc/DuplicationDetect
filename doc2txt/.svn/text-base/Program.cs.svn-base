using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using Core = Microsoft.Office.Core;
using PowerPoint;
using Word;
using System.Reflection;
using System.Diagnostics;

namespace doc_ppt2txt
{
    class Program
    {
        /*public static string ReplaceNonPrintCharacters(string tmp)
        {
            StringBuilder info = new StringBuilder();
            foreach (char cc in tmp)
            {
                if ((int)cc>=0 || (int)cc<=31)
                    info.AppendFormat("&#x{0:X};", (int)cc);
                else info.Append(cc);
            }
            return info.ToString();
        }*/
        static void Main(string[] args)
        {
           /* string s = "这些传输介质有以下特性：　·信息流方向　·传送信号的类型　·传输速率　·带宽";
            s = ReplaceNonPrintCharacters(s);*/
           // Console.Write(s);
            //Console.Read();
            if (args.Length != 1)
            {
                Console.Write("argument format error...");
                return;
            }
            String fileName = args[0];
            if (!System.IO.File.Exists(fileName))
            {
                Console.Write("the input file not exist...");
                return;
            }
            String dir = System.AppDomain.CurrentDomain.SetupInformation.ApplicationBase;
            //Console.Write(dir);
            int index = fileName.LastIndexOf('.') + 1;
            String fileFormat = fileName.Substring(index, fileName.Length - index);
            object docFile = "" as object;
            String pptFile = "";
            String tempFile = dir + @"\temp.txt";
            String txtcontent = "";
            if (fileFormat.Contains("ppt") || fileFormat.Contains("pot"))
            {
                pptFile = fileName;
                PowerPoint.Application app = new PowerPoint.ApplicationClass();
                PowerPoint.Presentation pre = app.Presentations.Open(pptFile, Core.MsoTriState.msoTrue,
                    Core.MsoTriState.msoTrue, Core.MsoTriState.msoFalse);
                StreamWriter sw = new StreamWriter(tempFile, false, Encoding.GetEncoding("gb2312"));
                foreach (PowerPoint.Slide sld in pre.Slides)
                {
                    foreach (PowerPoint.Shape sp in sld.Shapes)
                    {
                        if (sp.HasTextFrame == Core.MsoTriState.msoTrue)
                        {
                            int i = 1;
                            while (sp.TextFrame.TextRange.Paragraphs(i, 1).Text != "")
                            {
                                txtcontent = sp.TextFrame.TextRange.Paragraphs(i, 1).Text;
                                //txtcontent.
                                sw.WriteLine(sp.TextFrame.TextRange.Paragraphs(i, 1).Text);
                                i++;
                            }
                        }
                    }
                }
                sw.Close();
                pre.Close();
                app.Quit();
            }
            else if (fileFormat.Contains("doc") || fileFormat.Contains("dot"))
            {
                //deal with the word file
                object missingValue = System.Type.Missing;
                docFile = fileName as object;
                Word.Application wdApp = new Word.ApplicationClass();
                Word.Document wdDoc = wdApp.Documents.Open(ref docFile, ref missingValue,
                    ref missingValue, ref missingValue, ref missingValue,
                    ref missingValue, ref missingValue, ref missingValue,
                    ref missingValue, ref missingValue, ref missingValue,
                    ref missingValue, ref missingValue, ref missingValue,
                    ref missingValue, ref missingValue);
                int count = wdDoc.Paragraphs.Count;
                StreamWriter sw = new StreamWriter(tempFile, false, Encoding.GetEncoding("gb2312"));
                for (int i = 1; i <= count; i++)
                {
                    sw.WriteLine(wdDoc.Paragraphs[i].Range.Text);
                }
                sw.Close();
                wdDoc.Close(ref missingValue, ref missingValue, ref missingValue);
                wdApp.Quit(ref missingValue, ref missingValue, ref missingValue);
            }
            else
            {
                Console.Write("unknow file format!");
                return;
            }
            return;
        }
    }
}
