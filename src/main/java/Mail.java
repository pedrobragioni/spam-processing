package spam;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

public class Mail implements Writable {
	
	public String From;
	public String Reply_To;
	public String Mailer;
	public String Message_ID;
	public String Date;
	public String Organization;
	public String Mime_Version;
	public String Content_Type;
	public String Charset;
	public String Additional_Headers;
	public String Timestamp;
	public String Subject;
	public String SMTP_Proto;
	public String Mail_From;
	public Vector<String> Rcpt_to;
	public int SMTP_Count;
	public int Sensor_Dstport;
	public String Sensor_Designation;
	public String Sensor_CC;
	public String Src_Proto;
	public String Src_IP;
	public String Src_Hostname;
	public String Src_ASN;
	public String Src_OS;
	public String Src_RIR;
	public String Src_CC;
	public String Src_Dnsbl;
	public int Src_Srcport;
	public String Src_Prefix;
	public String Dst_IP;
	public String Dst_Hostname;
	public String Dst_ASN;
	public String Dst_Dstport;
	public String Dst_RIR;
	public String Dst_CC;
	public String Dst_Dnsbl;
	public String Dst_Prefix;
	public Vector<String> Content; // Only Plain and HTML
    public Vector<String> Parts_Content_Type; // Only Plain and HTML
    public Vector<String> All_Content; // All content
    public Vector<String> All_Parts_Content_Type; //All types of content

	
	  public Mail(String From, String Reply_To, String Mailer, String Message_ID, String Date, String Organization, 
			  String Mime_Version, String Content_Type, String Charset, String Additional_Headers, String Timestamp, String Subject, 
			  String SMTP_Proto, String Mail_From, Vector<String> Rcpt_to, int SMTP_Count, 
			  int Sensor_Dstport, String Sensor_Designation, String Sensor_CC, String Src_Proto, String Src_IP, 
			  String Src_Hostname, String Src_ASN, String Src_OS, String Src_RIR, String Src_CC, String Src_Dnsbl, 
			  int Src_Srcport, String Src_Prefix, String Dst_IP, String Dst_Hostname, String Dst_ASN, 
			  String Dst_Dstport, String Dst_RIR, String Dst_CC, String Dst_Dnsbl, String Dst_Prefix, Vector<String> Content,
              Vector<String> Parts_Content_Type, Vector<String> All_Content, Vector<String> All_Parts_Content_Type) {

		  	this.From = From;
		  	this.Reply_To = Reply_To;
		  	this.Mailer = Mailer;
		  	this.Message_ID = Message_ID;
		  	this.Date = Date;
		  	this.Organization = Organization;
		  	this.Mime_Version = Mime_Version;
			this.Content_Type = Content_Type;
			this.Charset = Charset;
			this.Additional_Headers = Additional_Headers;
			this.Timestamp = Timestamp;
			this.Subject = Subject;
			this.SMTP_Proto = SMTP_Proto;
			this.Mail_From = Mail_From;
			this.Rcpt_to = Rcpt_to;
			this.SMTP_Count = SMTP_Count;
			this.Sensor_Dstport = Sensor_Dstport;
			this.Sensor_Designation = Sensor_Designation;
			this.Sensor_CC = Sensor_CC;
			this.Src_Proto = Src_Proto;
			this.Src_IP = Src_IP;
			this.Src_Hostname = Src_Hostname;
			this.Src_ASN = Src_ASN;
			this.Src_OS = Src_OS;
			this.Src_RIR = Src_RIR;
			this.Src_CC = Src_CC;
			this.Src_Dnsbl = Src_Dnsbl;
			this.Src_Srcport = Src_Srcport;
			this.Src_Prefix = Src_Prefix;
			this.Dst_IP = Dst_IP;
			this.Dst_Hostname = Dst_Hostname;
			this.Dst_ASN = Dst_ASN;
			this.Dst_Dstport = Dst_Dstport;
			this.Dst_RIR = Dst_RIR;
			this.Dst_CC = Dst_CC;
			this.Dst_Dnsbl = Dst_Dnsbl;
			this.Dst_Prefix = Dst_Prefix;
			this.Content = Content;
            this.Parts_Content_Type = Parts_Content_Type;
            this.All_Content = All_Content;
            this.All_Parts_Content_Type = All_Parts_Content_Type;
		  
	  }

	  public Mail() {
			/*this(new Text(), new Text(), new Text(), new Text(), new Text(), new Text(), new Text(), new Text(), 
			new Text(), new Text(), new Text(), new Text(), new Text(), new Vector<Text>(), new IntWritable(0), 
			new IntWritable(0), new Text(), new Text(),  new Text(),  new Text(), new Text(), new Text(),  
			new Text(), new Text(), new Text(), new Text(), new IntWritable(0), new Text(), new Text(),  
			new Text(), new Text(), new IntWritable(0), new Text(), new Text(), new Text(), new Text(),  
			new Text());*/	
		  this("", "", "", "", "", "", "", "", "", "", "", "", "", "", new Vector<String>(), 0, 0, "", "", 
				  "", "", "", "", "", "", "", "", 0, "", "", "", "", "", "", "", "", "", new Vector<String>(), new Vector<String>(), new Vector<String>(), new Vector<String>());
	  }
	
	  public void write(DataOutput out) throws IOException {
			out.writeChars(From);
			out.writeChars(Reply_To);
			out.writeChars(Mailer);
			out.writeChars(Message_ID);
			out.writeChars(Date);
			out.writeChars(Organization);
			out.writeChars(Mime_Version);
			out.writeChars(Content_Type);
			out.writeChars(Charset);
			out.writeChars(Additional_Headers);
			out.writeChars(Timestamp);
			out.writeChars(Subject);
			out.writeChars(SMTP_Proto);
			out.writeChars(Mail_From);
			out.writeInt(SMTP_Count);
			out.writeInt(Sensor_Dstport);
			out.writeChars(Sensor_Designation);
			out.writeChars(Sensor_CC);
			out.writeChars(Src_Proto);
			out.writeChars(Src_IP);
			out.writeChars(Src_Hostname);
			out.writeChars(Src_ASN);
			out.writeChars(Src_OS);
			out.writeChars(Src_RIR);
			out.writeChars(Src_CC);
			out.writeChars(Src_Dnsbl);
			out.writeInt(Src_Srcport);
			out.writeChars(Src_Prefix);
			out.writeChars(Dst_IP);
			out.writeChars(Dst_Hostname);
			out.writeChars(Dst_ASN);
			out.writeChars(Dst_Dstport);
			out.writeChars(Dst_RIR);
			out.writeChars(Dst_CC);
			out.writeChars(Dst_Dnsbl);
			out.writeChars(Dst_Prefix);
			
			for (String l : Rcpt_to) {
		        out.writeChars(l);
		    }
			
			for (String z : Content) {
		        out.writeChars(z);
		    }

            for (String w : Parts_Content_Type) {
               out.writeChars(w);
            }

            for (String al : All_Content) {
               out.writeChars(al);
            }

            for (String a : All_Parts_Content_Type) {
               out.writeChars(a);
            }
	  }

	  public void readFields(DataInput in) throws IOException {
		
		  	 From = in.readLine();
			 Reply_To = in.readLine();
			 Mailer = in.readLine();
			 Message_ID = in.readLine();
			 Date = in.readLine();
			 Organization = in.readLine();
			 Mime_Version = in.readLine();
			 Content_Type = in.readLine();
			 Charset = in.readLine();
			 Additional_Headers = in.readLine();
			 Timestamp = in.readLine();
			 Subject = in.readLine();
			 SMTP_Proto = in.readLine();
			 Mail_From = in.readLine();
			 SMTP_Count = in.readInt();
			 Sensor_Dstport = in.readInt();
			 Sensor_Designation = in.readLine();
			 Sensor_CC = in.readLine();
			 Src_Proto = in.readLine();
			 Src_IP = in.readLine();
			 Src_Hostname = in.readLine();
			 Src_ASN = in.readLine();
			 Src_OS = in.readLine();
			 Src_RIR = in.readLine();
			 Src_CC = in.readLine();
			 Src_Dnsbl = in.readLine();
			 Src_Srcport = in.readInt();
			 Src_Prefix = in.readLine();
			 Dst_IP = in.readLine();
			 Dst_Hostname = in.readLine();
			 Dst_ASN = in.readLine();
			 Dst_Dstport = in.readLine();
			 Dst_RIR = in.readLine();
			 Dst_CC = in.readLine();
			 Dst_Dnsbl = in.readLine();
			 Dst_Prefix = in.readLine();
	  
			 for(int i = 0; i < Rcpt_to.size(); i++){
			   String f = "";
			   f = in.readLine();
			   Rcpt_to.add(f);
			 }
			 
			 for(int i = 0; i < Content.size(); i++){
			   String f = "";
			   f = in.readLine();
			   Content.add(f);
             }

             for(int i =0; i < Parts_Content_Type.size(); i++){
               String f = "";
               f = in.readLine();
               Parts_Content_Type.add(f);
             }

             for(int i = 0; i < All_Content.size(); i++){
               String f = "";
               f = in.readLine();
               All_Content.add(f);
             }

             for(int i = 0; i < All_Parts_Content_Type.size(); i++){
               String f = "";
               f = in.readLine();
               All_Parts_Content_Type.add(f);
             }
	  }
	
}
