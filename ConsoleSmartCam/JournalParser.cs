using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using ConsoleSmartCam.ConsoleSmartCamTableAdapters;
using SmartCamLibrary;

namespace ConsoleSmartCam
{
    public class JournalParser
    {
        public string AmountStr = string.Empty;
        public string Remark = string.Empty;
        public string CardNo = string.Empty;
        public string TransId = string.Empty;
        public string TerminalId = string.Empty;
        public string TransDate = string.Empty;
        public string TransTime = string.Empty;
        public string CashTaken = string.Empty;
        public string CashPresented = string.Empty;
        public string CardTaken = string.Empty;
        public string CardEntered = string.Empty;
        public string CardEjected = string.Empty;
        public string JournalPart = string.Empty;
        public string MsgValue = string.Empty;
        public int MsgType = 0;
        public string[] ReturnParsed;
        int bp = 0, ct = 0, ce = 0;
        private DataTable _dt;
        private RecievedDataTableAdapter _rdTa;

        TransSession trans;
        TransSession passedTrans = new TransSession();
        NoParseJournal _noParse = new NoParseJournal();
        private TransSessionTableAdapter _sessTa;
        private TerminalProvisionTableAdapter _TpTableAdapter;
        private DateTime dDate;
        private string transType;

        public DataTable GetUnParsedMessage()
        {

            int recId = 0;
            int msgType = 0;
            string unparsed = String.Empty;
            _dt = new DataTable();
            _rdTa = new RecievedDataTableAdapter();
            _dt = _rdTa.GetData();
            for (int i = 0; i < _dt.Rows.Count; i++)
            {
                try
                {
                    recId = Convert.ToInt32(_dt.Rows[i][0]);
                    //split and deserialize xml string
                    unparsed = _dt.Rows[i][1].ToString();
                    byte[] array = Encoding.ASCII.GetBytes(unparsed);
                    if (array != null && array.Length > 0)
                    {
                        if (!string.IsNullOrEmpty(unparsed) || !string.IsNullOrWhiteSpace(unparsed))
                        {
                            string[] unparsed1 = StringSplit(unparsed, "<EOF>");
                            foreach (var s in unparsed1)
                            {
                                if (s != "")
                                {
                                    string[] s1 = s.Split('|');
                                    if (s1.Any())
                                    {
                                        MsgType = Convert.ToInt32(s1[0]);
                                        MsgValue = s1[1].ToString();
                                    }
                                    switch (MsgType)
                                    {
                                        case 1:
                                            //TODO: Images message
                                            ///ProcessImagesMsg(msg);
                                            break;
                                        case 2:
                                            //TODO: Session message
                                            ProcessSessionMsg(MsgValue, recId);
                                            break;
                                        case 3:
                                            //TODO: camera message
                                            // ProcessCameraMsg(msg);
                                            break;
                                        case 4:
                                            //TODO: Terminal Provision Message
                                            ProcessTerminalProvisionMsg(MsgValue, recId);
                                            break;
                                        case 5:
                                            //TODO: ProcessCallMaintenanceMessage
                                            // ProcessMaintenanceMessage(msg);
                                            break;

                                        default:
                                            Console.WriteLine(@"Unknown function recieved!");
                                            break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            DeleteRecordFromTable(recId);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Err reading dt : " + ex.Message);
                }

            }
            return null;
        }

        TransSession ParseDiebold(string unp, TransSession oTrans)
        {
            trans = oTrans;
            unp = unp.Remove(0, 9);
            string[] upArr = unp.Split('\n');
            if (upArr.Any())
            {
                string fst = upArr[0];
                char[] ch1 = new[] { ' ', '\t' };
                string[] sp1 = fst.Split(ch1);
                char[] dtch = new[] { '\\' };
                string[] dtSp = sp1[4].Split(dtch);
                string year = dtSp[2], month = dtSp[1], day = dtSp[0];
                string time = sp1[9];
                DateTime dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                trans.SessionStartTime = dDate.Date.ToString();
                trans.TranDate = dDate.ToString();
                trans.TerminalId = sp1[14];

                string snd = upArr[1];
                trans.CardNo = snd.Remove(0, 13).Trim();

                string trd = upArr[2];
                char[] ch2 = new[] { ' ', '\t' };
                string[] sp2 = trd.Split(ch2);
                trans.TransId = sp2[0];

                string fth = upArr[3];
                char[] ch3 = new[] { ' ', '\t' };
                string[] sp3 = fth.Split(ch3);
                int cSp3 = sp3.Count();
                if (cSp3 > 1)
                {
                    string oAmount = sp3[cSp3 - 1];
                    trans.Amount = sp3[cSp3 - 1].Remove(0, 3);
                    decimal d;
                    if (decimal.TryParse(trans.Amount, out d))
                    {
                        trans.Amount = trans.Amount;
                        trans.AmountDouble = Convert.ToDouble(trans.Amount);
                    }
                    else
                    {
                        trans.Amount = null;
                        trans.AmountDouble = 0.0;
                    }
                    Remark = fth.ToString().Remove(fth.Length - oAmount.Length);
                    trans.TransType = Remark.Trim();
                }
                else
                {
                    trans.TransType = sp3[0].ToString();
                }
                //trans.TransType = Remark;
                if (!Remark.Contains("WITHDRAW") || !Remark.Contains("INQUIRY") || !Remark.Contains("TRANSFER"))
                {
                    trans.Remark = sp3[0].ToString();
                }


            }
            return trans;
        }

        TransSession ParseWincor1(string unp)
        {
            trans = new TransSession();
            try
            {
                string[] upArr = unp.Split('\n');
                char[] dtch = new[] { ' ', '\t' };
                char[] dtch1 = new[] { ' ', '\\' };
                if (upArr.Any())
                {
                    for (int a = 0; a < upArr.Count(); a++)
                    {
                        string aline = upArr[a];
                        if (aline.StartsWith("CARD NUMBER"))
                        {
                            trans.CardNo = aline.Remove(0, 13);
                            string[] dtLine = upArr[a - 1].Split(dtch);
                            string[] dt = dtLine[4].Split(dtch1);
                            string year = dt[2], month = dt[1], day = dt[0];
                            string time = dtLine[9];
                            dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                            trans.TranDate = dDate.ToString();
                            trans.TerminalId = dtLine[14];

                            //trans id line
                            string[] transId = upArr[a + 1].Split(dtch);
                            trans.TransId = transId[0];

                            //trans type line
                            string tt = upArr[a + 1];
                            if (tt.StartsWith("WITHDRAW") || tt.StartsWith("INQUIRY"))
                            {
                                string[] amtArr = tt.Split(dtch);
                                if (amtArr.Count() == 1)
                                {
                                    trans.TransType = "INQUIRY";
                                }
                                else
                                {
                                    trans.TransType = "WITHDRAW";
                                    string amt = amtArr[amtArr.Count() - 1];
                                    trans.Amount = amt;
                                    trans.AmountDouble = Convert.ToDouble(trans.Amount.Remove(0, 3));
                                }
                            }
                            else
                            {
                                trans.TransType = "***UNKNOW***";
                                trans.TransType = "WINCOR";
                            }
                        }

                        trans.Remark = trans.TransType;
                        trans.JournalPart = unp;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("WINCOR PARSING ERR1 : " + ex.Message);
            }

            return trans;
        }
        private TransSession ParseWincor(string unp)
        {
            trans = new TransSession();
            //unp = unp.Remove(0, 9);
            string[] upArr = unp.Split('\n');
            if (upArr.Any())
            {
                for (int a = 0; a < upArr.Count(); a++)
                {
                    string aline = upArr[a];
                    if (aline.StartsWith("INQUIRY"))
                    {
                        trans.TransType = "INQUIRY";
                        trans.JournalPart = unp;
                        //trans date and time
                        char[] dtch = new[] { ' ', '\t' };
                        char[] dtch1 = new[] { ' ', '\\' };
                        string[] dtStr = upArr[1].Split(dtch);
                        string[] dtSp = dtStr[4].Split(dtch1);
                        string year = dtSp[2], month = dtSp[1], day = dtSp[0];
                        string time = dtStr[9];
                        dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                        trans.TranDate = dDate.ToString();
                        trans.TerminalId = dtStr[14];

                        string[] transId = upArr[3].Split(dtch);
                        trans.TransId = transId[0];

                        for (int i = 0; i < upArr.Count(); i++)
                        {
                            string d = upArr[i];
                            if (d.Contains("CARD NUMBER"))
                            {
                                trans.CardNo = d.Remove(0, 13).Trim();
                            }
                            trans.TransType = "INQUIRY";
                            trans.Remark = trans.TransType;
                        }
                    }
                    else if (aline.StartsWith("THE TRANSACTION COULD"))
                    {
                        char[] dtch = new[] { ' ', '\t' };
                        char[] dtch1 = new[] { ' ', '\\' };
                        for (int i = 0; i < upArr.Count(); i++)
                        {
                            string d = upArr[i];
                            if (d.Contains("CARD NUMBER"))
                            {
                                string tId = upArr[i - 1];
                                string[] tId1 = tId.Split(dtch);
                                string[] dtSp = tId1[4].Split(dtch1);
                                string year = dtSp[2], month = dtSp[1], day = dtSp[0];
                                string time = tId1[9];
                                dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                                trans.TranDate = dDate.ToString();
                                trans.TerminalId = tId1[14];
                                trans.TransType = "***UNKNOWN***";
                                string[] transId = upArr[3].Split(dtch);
                                trans.TransId = transId[0];
                                trans.Remark = "THE TRANSACTION COULD COMPLETED...";
                                trans.JournalPart = unp;
                            }
                        }
                    }
                    else if (aline.StartsWith("ADVANCE PREPAID"))
                    {
                        char[] dtch = new[] { ' ', '\t' };
                        char[] dtch1 = new[] { ' ', '\\' };
                        for (int i = 0; i < upArr.Count(); i++)
                        {
                            string d = upArr[i];
                            if (d.Contains("CARD NUMBER"))
                            {
                                string tId = upArr[i - 1];
                                string[] tId1 = tId.Split(dtch);
                                string[] dtSp = tId1[4].Split(dtch1);
                                string year = dtSp[2], month = dtSp[1], day = dtSp[0];
                                string time = tId1[9];
                                dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                                trans.TranDate = dDate.ToString();
                                trans.TerminalId = tId1[14];

                                string[] transId = upArr[i + 1].Split(dtch);
                                trans.TransId = transId[0];
                                trans.TransType = "ADVANCE PREPAID";
                                trans.Remark = "ADVANCE PREPAID - VIRTUAL TOP UP";
                                trans.JournalPart = unp;
                            }
                        }
                    }
                    else if (aline.StartsWith("WITHDRAW"))
                    {
                        try
                        {
                            trans.TerminalType = _noParse.Mtype;
                            if (unp.Contains("CASH PRESENTED"))
                            {
                                trans.BillPresented = 1;
                            }
                            if (unp.Contains("CASH") && unp.Contains("TAKEN"))
                            {
                                trans.BillTaken = 1;
                            }
                            if (unp.Contains("CARD") && unp.Contains("TAKEN"))
                            {
                                //trans.CardTaken = 1;
                            }
                            trans.TransType = "WITHDRAW";
                            trans.JournalPart = unp;
                            //note bills
                            trans.NoteBills = upArr[2].Trim().Remove(0, 14);
                            //trans date and time
                            char[] dtch = new[] { ' ', '\t' };
                            char[] dtch1 = new[] { ' ', '\\' };
                            string[] dtStr = upArr[4].Split(dtch);
                            string[] dtSp = dtStr[4].Split(dtch1);
                            string year = dtSp[2], month = dtSp[1], day = dtSp[0];
                            string time = dtStr[9];
                            dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                            trans.SessionStartTime = dDate.Date.ToString();
                            trans.TranDate = dDate.ToString();
                            TerminalId = dtStr[14];
                            trans.TerminalId = TerminalId;

                            CardNo = upArr[5].Remove(0, 13).Trim();
                            trans.CardNo = CardNo;
                            string[] transId = upArr[6].Split(dtch);
                            trans.TransId = transId[0];
                            //string[] amtArr = upArr[7].Split(dtch);
                            //trans.Amount = amtArr[amtArr.Count() - 1];
                            //trans.AmountDouble = Convert.ToDouble(trans.Amount.Remove(0, 3));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("WITHDRAW CERR : " + ex.Message);
                            for (int i = 0; i < upArr.Count(); i++)
                            {
                                string unknown = upArr[i];
                                if (!unknown.StartsWith("WITHDRAW"))
                                {
                                    if (unknown.Contains("CARD NUMBER"))
                                    {
                                        char[] dtch = new[] { ' ', '\t' };
                                        char[] dtch1 = new[] { ' ', '\\' };
                                        string tId = upArr[i - 1];
                                        string[] tId1 = tId.Split(dtch);
                                        string[] dtSp = tId1[4].Split(dtch1);
                                        string year = dtSp[2], month = dtSp[1], day = dtSp[0];
                                        string time = tId1[9];
                                        dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                                        trans.TranDate = dDate.ToString();
                                        trans.TerminalId = tId1[14];

                                        string[] transId = upArr[i + 1].Split(dtch);
                                        trans.TransId = transId[0];
                                        trans.TransType = "***UNKNOWN***";
                                        trans.Remark = trans.TransType;
                                        trans.JournalPart = unp;
                                        trans.TerminalType = "WINCOR";
                                    }
                                }
                            }
                        }
                    }
                    else if (aline.StartsWith("YOU HAVE EXCEEDED YOUR"))
                    {
                        for (int i = 0; i < upArr.Count(); i++)
                        {
                            string unknown = upArr[i];
                            if (unknown.Contains("CARD NUMBER"))
                            {
                                char[] dtch = new[] { ' ', '\t' };
                                char[] dtch1 = new[] { ' ', '\\' };
                                string[] tId = upArr[i + 1].Split(dtch);
                                trans.TransId = tId[0];
                                string[] termId = upArr[i - 1].Split(dtch);
                                string[] dtStr = upArr[4].Split(dtch);
                                string[] dtSp = dtStr[4].Split(dtch1);
                                string year = dtSp[2], month = dtSp[1], day = dtSp[0];
                                string time = dtStr[9];
                                dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                                trans.TranDate = dDate.ToString();
                                trans.TerminalId = termId[(tId.Count() - 1)];
                                trans.TransType = "***UNKNOWN***";
                                trans.Remark = trans.TransType;
                                trans.JournalPart = unp;
                                trans.TerminalType = "WINCOR";
                            }
                        }
                    }
                    else
                    {
                        for (int i = 0; i < upArr.Count(); i++)
                        {
                            try
                            {
                                string unknown = upArr[i];
                                if (unknown.Contains("CARD NUMBER"))
                                {
                                    char[] dtch = new[] { ' ', '\t' };
                                    char[] dtch1 = new[] { ' ', '\\' };
                                    string[] tId = upArr[i + 1].Split(dtch);
                                    trans.TransId = tId[0];

                                    string[] termId = upArr[i - 1].Split(dtch);
                                    string[] dtStr = termId[4].Split(dtch1);
                                    //string[] dtSp = dtStr[4].Split(dtch1);
                                    string year = dtStr[2], month = dtStr[1], day = dtStr[0];
                                    string time = termId[9];
                                    dDate = DateTime.Parse(year + "-" + month + "-" + day + " " + time);
                                    trans.TranDate = dDate.ToString();
                                    trans.TerminalId = termId[(termId.Count() - 1)];
                                    trans.TransType = "***UNKNOWN***";
                                    trans.Remark = trans.TransType;
                                    trans.JournalPart = unp;
                                    trans.TerminalType = "WINCOR";
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("OTHER ELSE CERR : " + ex.Message);
                            }

                        }

                    }
                }
                trans.TerminalType = _noParse.Mtype;
            }
            return trans;
        }

        void ParseHyosung(string unp)
        {
            unp = unp.Remove(0, 9);
            string[] upArr = unp.Split('\n');
            if (upArr.Any())
            {
                string fst = upArr[0];
                char[] ch = new[] { ' ', '\t' };
                string[] sp = fst.Split(ch);
                TransDate = sp[0].ToString();
                TransTime = sp[1].ToString();
                TerminalId = sp[2].ToString();
            }
        }

        public void ProcessSessionMsg(string msg, int recId)
        {
            _noParse = DeSerializeObject(msg);
            if (_noParse != null)
            {
                if (_noParse.IsCashPresented == "Yes")
                {
                    bp = 1;
                }
                if (_noParse.IsCashtaken == "Yes")
                {
                    ct = 1;
                }
                if (_noParse.IsCardEjected == "Yes")
                {
                    ce = 1;
                }
                if (_noParse.IsCardEjected == "Yes")
                {
                    ce = 1;
                }


                if (_noParse.Mtype.Contains("DIEBOLD"))
                {
                    trans = new TransSession();
                    trans.TerminalType = _noParse.Mtype;
                    trans.BillPresented = bp;
                    trans.BillTaken = ct;
                    trans.NoteBills = _noParse.NoteBills;

                    passedTrans = ParseDiebold(_noParse.Jpart, trans);
                }
                else if (_noParse.Mtype.Contains("WINCOR"))
                {
                    passedTrans = ParseWincor1(_noParse.Jpart);

                }
            }

            if (string.IsNullOrEmpty(passedTrans.TransId) || passedTrans.TransId == "")
            {
                DeleteRecordFromTable(recId);
            }
            else
            {
                _sessTa = new TransSessionTableAdapter();
                int insert = _sessTa.Insert(passedTrans.TerminalId, passedTrans.TerminalType, null, null, passedTrans.TransType,
             passedTrans.NoteBills, null, null, passedTrans.TransId, passedTrans.BillTaken, passedTrans.BillPresented, passedTrans.CardNo, null,
             null, passedTrans.Amount, null, null, passedTrans.JournalPart, passedTrans.TranDate, null, null, DateTime.Now,
             passedTrans.Remark, null, null, null, null, null, null, null, null, null, null, null, Convert.ToDecimal(passedTrans.AmountDouble));

                if (insert > 0)
                {
                    Console.WriteLine("Record insert successfull...");
                    //do delete of record from parent table
                    DeleteRecordFromTable(recId);
                }
            }



        }

        private void ProcessTerminalProvisionMsg(string msg, int recId)
        {
            Messages messages = new Messages();
            TerminalProvision terminal = new TerminalProvision();
            terminal = messages.DeSerializeProvision(msg);
            if (terminal != null)
            {
                if (terminal.TerminalId != String.Empty)
                {
                    _TpTableAdapter = new TerminalProvisionTableAdapter();
                    int ins = _TpTableAdapter.Insert(terminal.TerminalId, terminal.TerminalType,
                        terminal.TerminalIp, terminal.RemoteIp, terminal.Name, terminal.AliasName,
                        terminal.Location, terminal.JournalPath, terminal.ImagePath, terminal.CustodianName,
                        terminal.Phone, terminal.Email, terminal.Address, terminal.EntryDate, null);

                    if (ins > 0)
                    {
                        Console.WriteLine("Terminal Provision Inserted...");
                        DeleteRecordFromTable(recId);
                    }
                }
            }
        }


        private void DeleteRecordFromTable(int recId)
        {
            _sessTa = new TransSessionTableAdapter();
            int d = _sessTa.DeleteById(recId);
            if (d > 0)
            {
                Console.WriteLine("Record deleted from parent table...");
            }
        }

        public NoParseJournal DeSerializeObject(string str)
        {
            try
            {
                using (TextReader textReader = (TextReader)new StringReader(str))
                {
                    XmlSerializer ser = new XmlSerializer(typeof(NoParseJournal));
                    return (NoParseJournal)ser.Deserialize(textReader);

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return (NoParseJournal)null;
            }
        }


        public string[] StringSplit(string s, string separator)
        {
            return s.Split(new string[] { separator }, StringSplitOptions.None);
        }
    }

    public class JournalMessage
    {
        public string Mtype { get; set; }
        public string IsCashtaken { get; set; }
        public string IsCashPresented { get; set; }
        public string IsCardTaken { get; set; }
        public string IsCardEjected { get; set; }
        public string Jpart { get; set; }

    }
    public class NoParseJournal
    {
        public string Mtype { get; set; }
        public string NoteBills { get; set; }
        public string Jpart { get; set; }
        public string IsCashtaken { get; set; }
        public string IsCashPresented { get; set; }
        public string IsCardEjected { get; set; }
        public string IsCardTaken { get; set; }

    }
}
