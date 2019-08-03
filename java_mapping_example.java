package com.gwl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.aii.mapping.api.StreamTransformationException;
import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.TransformationOutput;


/**
 * Extracts header and footer XML payloads and maps it to the downstream structure.
 */
public class ExtractHeaderFooter extends AbstractTransformation {

	private String lineItemName;
	private String nameSpace;
	private String messageName;
	
	@SuppressWarnings("serial")
	public static final Map<String, String> headerMap = new HashMap<String, String> () {{
		put("RECTYPE", "RECTYPE");
		put("OI_IDENTIFIER", "SOURCE");
		put("INVOICE_NBR", "OBJTYPE");
		put("OI_LINE_NBR", "CycleNumber");
		put("BLTY_BILLING_TYPE", "FileStamp");
	}};
	
	@SuppressWarnings("serial")
	public static final Map<String, String> footerMap = new HashMap<String, String> () {{
		put("RECTYPE", "RECTYPE");
		put("OI_IDENTIFIER", "SOURCE");
		put("INVOICE_NBR", "SEQNUM");
		put("OI_LINE_NBR", "FILESTAMP");
		put("BLTY_BILLING_TYPE", "RECCOUNT");
		put("EXTERNALPARTYID", "RECAMT");
	}};
	
	public ExtractHeaderFooter() {
		
	}
	
	public ExtractHeaderFooter(String lineItemName, String nameSpace, String messageName) {
		this.lineItemName = lineItemName;
		this.nameSpace = nameSpace;
		this.messageName = messageName;
	}
		
	/**
	 * Instantiates Document from string XML payload.
	 *
	 * @param payload XML payload
	 *
	 * @return Document DOM object
	 */
	private Document getDocument(String payload) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = builderFactory.newDocumentBuilder();
		Document xmlDocument = builder.parse(new ByteArrayInputStream(payload.getBytes()));
		return xmlDocument;
	}
	
	/**
	 * Gets name of the node at the given context, either Header, LineItem or Footer are returned.
	 *
	 * @param index number of the given node
	 * @param numberOfLineItems lineitem count
	 */
	public String getNodeItemName(int index, int numberOfLineItems) {
		
		String rtn = "";
		if(index == 0) {
			rtn = "Header";
			
		} else if(index == (numberOfLineItems - 1)) {
			rtn = "Footer";
			
		} else {
			rtn = "LineItems";
		}
		
		return rtn;
	}
	
	/**
	 * Gets parameters from inputStream and sets the globally.
	 *
	 * @param inputStream inputStream
	 */
	public void setParameters(TransformationInput inputStream) {
		this.lineItemName = inputStream.getInputParameters().getString("LINE_ITEM_NAME");
		this.nameSpace = inputStream.getInputParameters().getString("NAMESPACE");
		this.messageName = inputStream.getInputParameters().getString("MESSAGE_NAME");
	}
	
	
	/**
	 *  Returns a particular field name value from the header string (of a CVS file).
	 *
	 *  @param fieldName Name of the field to look for.
	 *  @param index field number in the CSV header 0 to recordCount
	 *  @param recordCount nunber of fields in header
	 *
	 *  @return rtn detemined string value.
	 */
	public String getFieldName(String fieldName, int index, int recordCount) {
		
		String rtn = fieldName;
		if(index == 0) {
			rtn = this.headerMap.get(fieldName);
		} else if(index == (recordCount - 1)) {
			rtn = this.footerMap.get(fieldName);
		} 
		
		return rtn;
	}
	
	/**
	 *  Contains main transformation logic.
	 *
	 *  @param InputStream input parameter stream
	 *  @param OutputSttream output parameter stream
	 *
	 */
	public void execute(InputStream is, OutputStream os) throws ParserConfigurationException, SAXException, IOException {
		
		byte[] b = new byte[is.available()];
		is.read(b);
		String XMLinputFileContent = new String(b);
		
		Document doc = this.getDocument(XMLinputFileContent);
		doc.getDocumentElement().normalize();
		
		NodeList lineItems = doc.getElementsByTagName(this.lineItemName);
		
		String payload = ""
			+ "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
			"<ns0:" + this.messageName + " xmlns:ns0=\"" + this.nameSpace + "\">";
		
		int lineItemCount = lineItems.getLength();
		for(int i = 0; i < lineItemCount; i++) {
			
			String nodeName = getNodeItemName(i, lineItems.getLength());
			Node lineItem = lineItems.item(i);
			if(lineItem.getNodeType() == Node.ELEMENT_NODE) {
				
				payload += "<" + nodeName + ">";
				NodeList lineItemFields = lineItem.getChildNodes();
				for(int k = 0; k < lineItemFields.getLength(); k++) {
					
					Node lineItemField = lineItemFields.item(k);
					if(lineItemField.getNodeType() == Node.ELEMENT_NODE) {
						
						String fieldName = this.getFieldName(lineItemField.getNodeName(), i, lineItemCount);
						
						payload += "<" + fieldName + ">";
						payload += lineItemField.getTextContent();
						payload += "</" + fieldName + ">";
					}
				}
				payload += "</" + nodeName + ">";
			}
		}
		
		payload += "</ns0:" + this.messageName + ">";
		os.write(payload.getBytes());
	}
	
	/**
	 *  Entry point of application. Converts PI specific streams into regular IO streams and runs execute.
	 *  This approach simplifies testing.
	 *
	 *	@param TransformationInput input parameters.
	 *  @param TransformationOutput stream that contains return values.
	 */
	public void transform(TransformationInput transformationInput, TransformationOutput transformationOutput) throws StreamTransformationException {
		try {
			InputStream inputstream = transformationInput.getInputPayload().getInputStream();
			OutputStream outputstream = transformationOutput.getOutputPayload().getOutputStream();
			this.setParameters(transformationInput);
			this.execute(inputstream, outputstream);
			
		} catch(Exception e) {
			this.getTrace().addInfo(e.toString());
			e.printStackTrace();
		}
	}
}
