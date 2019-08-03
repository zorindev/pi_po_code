package com.sap.xi.tf;

import com.sap.aii.mapping.lookup.*;
import java.util.*;
import com.sap.aii.mappingtool.tf7.rt.*;
import java.io.*;
import com.sap.aii.mapping.api.*;
import java.lang.reflect.*;
import java.text.*;
import com.sap.ide.esr.tools.mapping.core.ExecutionType;
import com.sap.ide.esr.tools.mapping.core.Argument;
import com.sap.ide.esr.tools.mapping.core.Cleanup;
import com.sap.ide.esr.tools.mapping.core.LibraryMethod;
import com.sap.ide.esr.tools.mapping.core.Init;


/**
 * Example of a User Defined Mapping at a Queue level.
 *
 * This example was developed in NWDS in order to make it runnable testible as a standalone component.
 * When deployed to PI, the main method needs to be commented out.
 */
public class _recordFilter_  {


	static class TResultList implements ResultList {

		public int size;
		public int index;
		public List<String> values;
		
		public TResultList() {
			values = new ArrayList<String>();
			size = 0;
			index = 0;
		}
		
		@Override
		public void addContextChange() {
		}

		@Override
		public void addSuppress() {
		}

		@Override
		public void addValue(Object value) {
			values.add((String) value);
			size += 1;
		}

		@Override
		public void clear() {
			values = new ArrayList<String>();
			index = 0;
			size = 0;
		}
		
		public String next() {
			int i = 0;
			String rtn = "";
			for(String value: values) {
				if(i < size) {
					rtn = value;
				} 
				  
				if(i >= index) {
					break;
				}
				i++;
			}
			index += 1;
			return rtn;
		}
		
		public int size() {
			return size;
		}
	}

	class Record {
    	
		public int originalOrder;
		
    	public String pernr;
    	public String subtype;
    	public String enddate;
    	public String begindate;
    	public String userid;
    	public String useridLong;
    	
    	public String use;
    	public String status;
    	public String statusMessage;
    	
    	public Date beginDate;
    	public Date endDate;
    	public Boolean datesSet;
    	public Date compareDate;
    	
    	public Record(int originalOrder, String pernr, String subtype, String enddate, String begindate, String userid, String useridLong) {
    		this.originalOrder = originalOrder;
    		
    		this.pernr = pernr;
        	this.subtype = subtype;
        	this.enddate = enddate; 
        	this.begindate = begindate;
            this.userid = userid;
        	this.useridLong = useridLong;
        	
        	this.use = "y";
        	this.status = "";
        	this.statusMessage = "";
        	
        	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        	try {
        		this.beginDate = formatter.parse(this.begindate);
        		this.endDate = formatter.parse(this.enddate);
        		datesSet = true;
        	} catch(ParseException prse) {
        		datesSet = false;
    		}
    	}
    	
    	public void setCompareDate(String dateType) {
    		if(dateType.equals("begin")) {
    			this.compareDate = this.beginDate;
    		} else {
    			this.compareDate = this.endDate;
    		}
    	}
    	
    	public Date getCompareDate() {
    		return this.compareDate;
    	}
    }

	class DateComparator implements Comparator<Record> {

		@Override
		public int compare(Record record1, Record record2) {
			if(record1.datesSet && record2.datesSet) {
				return record1.getCompareDate().compareTo(record2.getCompareDate());
			} else {
				return 0;
			}
		}
	}

	class OrderComparator implements Comparator<Record> {
		
		@Override
		public int compare(Record record1, Record record2) {
			if(record1.originalOrder == record2.originalOrder) {
				return 0;
			} else if(record1.originalOrder < record2.originalOrder) {
				return -1;
			} else if(record1.originalOrder > record2.originalOrder) {
				return 1;
			}
			return 0;
		}
	}

	public AbstractTrace trace;

	public boolean isNumeric(String str) {  
        try {  
        	double d = Double.parseDouble(str);  
        } catch(NumberFormatException nfe) {  
        	return false;  
        }  
        return true;  
    }

	@SuppressWarnings("unchecked")
	public void execute(
		String[] ids,
		String[] subtypes,
		String[] enddates,
		String[] begindates,
		ResultList r_id,
		ResultList r_subtype,
		ResultList r_enddate,
		ResultList r_begindate,
		ResultList r_use,
		ResultList r_status,
		ResultList r_statusMessage) {
		
		Boolean doFiltering = true;
		Map<String, List<Record>> allRecords = new HashMap<String, List<Record>>();
		
		// organize records
		for(int i = 0; i < ids.length; i++) {
			Record record = new Record(
				(i + 1),
				ids[i],
				subtypes[i],
				enddates[i],
				begindates[i], 
			);
			record.setCompareDate("end");
			
			List<Record> RecordList = null;
			if(allRecords.containsKey(ids[i])) {
				try {					
					allRecords.get(ids[i]).add(record);
				} catch(NullPointerException npe) {
					doFiltering = false;
					break;
				}
			} else {
				RecordList = new ArrayList<Record>();
				RecordList.add(record);
				allRecords.put(ids[i], RecordList);
			}
		}
		
		
		
		int trackWritten = 0;
		Iterator<?> it = allRecords.entrySet().iterator();
		while(it.hasNext()) {
			@SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry)it.next();
			String x_id = (String) entry.getKey();
			@SuppressWarnings("unchecked")
			List<Record> Records = (ArrayList<Record>) entry.getValue();
			
			for(Record record: Records) {				    
				r_id.addValue(record.pernr);
				r_subtype.addValue(record.subtype);
				r_enddate.addValue(record.enddate);
				r_begindate.addValue(record.begindate);
				r_userid.addValue(record.userid);
				r_useridlong.addValue(record.useridLong);
				r_use.addValue(record.use);
				r_status.addValue(record.status);
				r_statusMessage.addValue(record.statusMessage);
				
				trackWritten += 1;
			}
		}
		
		
		// perform filtering
		if(doFiltering) {

			Iterator<?> it = allRecords.entrySet().iterator();
			while(it.hasNext()) {
				// get record
				@SuppressWarnings("rawtypes")
				Map.Entry entry = (Map.Entry)it.next();
				String x_id = (String) entry.getKey();
				@SuppressWarnings("unchecked")
				List<Record> Records = (ArrayList<Record>) entry.getValue();
				
				if(x_id.length() == 8 && this.isNumeric(x_id)) {

					if(Records.size() > 1) {  
	
						// sort all records for a given x_id by 
						Collections.sort(Records, new DateComparator());
						 
						// check for duplicate end dates
						Boolean duplicateEndDate 				= false;
						Boolean largestEndDateIsLessThanToday 	= false;
						Record trackedEndDateRecord 	= Records.get(Records.size() - 1);
						
						if(trackedEndDateRecord.endDate.compareTo(new Date()) < 0) {
							largestEndDateIsLessThanToday = true;
						}
						
						
						int index = 0;
						for(Record record: Records) {
							try {
								if(trackedEndDateRecord.endDate.equals(record.endDate) && index < (Records.size() - 1)) {
									duplicateEndDate = true;
								} 
							} catch(NullPointerException e) {
								
							}
							index += 1;
						}
						 
						// set all but the largest end date records to not be used in the reports
						index = 0;
						if((duplicateEndDate == false) && (largestEndDateIsLessThanToday == true)) {
							for(Record record: Records) {
								if(index < (Records.size() - 1)) {
									record.use = "n";
								}
								index += 1;
							}
						} else if((duplicateEndDate == false) && (largestEndDateIsLessThanToday == false)) {
							for(Record record: Records) {
								if(index < (Records.size() - 1)) {
									record.status = "e";
								} else {
									record.status = "w";
								}
								record.use = "y";
								record.statusMessage = "Multiple Records - Same ID Number & end date";
								index += 1;
							}
							
						} else {  // duplicate end date was found
							 	 
							// sort records by the start date
							for(Record record: Records) {
								record.setCompareDate("begin");
							}
							Collections.sort(Records, new DateComparator());
							 
							// check for duplicate start dates
							index = 0;
							Boolean duplicateStartDate = false;
							Record trackedx_idStartDateRecord = Records.get(0);
							for(Record record: Records) {
								if(index == 0) {
									index += 1;
									continue;
								}
								
								try {
									if(trackedx_idStartDateRecord.beginDate.equals(record.beginDate)) {
										duplicateStartDate = true;
									}
								} catch(NullPointerException e) {
									
								}
								 
								index += 1;
							}
							
							//mark duplicate records
							Collections.sort(Records, new OrderComparator());
							Boolean useableRecordHasBeenIdentified = false;
							
							for(Record record: Records) {
								
								try {
									if(duplicateStartDate) {
										if(record.beginDate.equals(trackedx_idStartDateRecord.beginDate) && !(useableRecordHasBeenIdentified)) {
											record.status = "w";
											useableRecordHasBeenIdentified = true;
										} else {
											record.status = "e";
										}
										record.use = "y";
										record.statusMessage = "Multiple Records - Same ID Number";
									} else {
										if(record.beginDate.equals(trackedx_idStartDateRecord.beginDate) && !(useableRecordHasBeenIdentified)) {
											record.status = "w";
											useableRecordHasBeenIdentified = true;
										} else {
											record.status = "e";
										}
										record.use = "y";
										record.statusMessage = "Multiple Records - Same ID Number & end date";
									}
								} catch(NullPointerException e) {
									this.trace.addDebugMessage(e.toString());
								}
							}
						} 
					} 
				}
				
				// restore the order
				Collections.sort(Records, new OrderComparator());
				
				// write out records
				for(Record record: Records) {
					
					if(record.use.equals("y")) {
						
						r_id.addValue(record.pernr);
						r_subtype.addValue(record.subtype);
						r_enddate.addValue(record.enddate);
						r_begindate.addValue(record.begindate);
						r_userid.addValue(record.userid);
						r_useridlong.addValue(record.useridLong);
						r_use.addValue(record.use);
						r_status.addValue(record.status);
						r_statusMessage.addValue(record.statusMessage);
						
						r_id.addContextChange();
						r_subtype.addContextChange();
						r_enddate.addContextChange();
						r_begindate.addContextChange();
						r_userid.addContextChange();
						r_useridlong.addContextChange();
						r_use.addContextChange();
						r_status.addContextChange();
						r_statusMessage.addContextChange();
						
					} else {
						
						r_id.addSuppress();
						r_subtype.addSuppress();
						r_enddate.addSuppress();
						r_begindate.addSuppress();
						r_userid.addSuppress();
						r_useridlong.addSuppress();
						r_use.addSuppress();
						r_status.addSuppress();
						r_statusMessage.addSuppress();
					}
				}
			}
		} 
	}

	/**
	 * Standard main method, used for testing of the execute method outside of PI.
	 * This method needs to be commented out or renamed in order for this code to be deployable to PI.
	 *
	 * @param args array of command line arguments
	 */
	public static void main(String [] args) {
		_recordFilter_ inst = new _recordFilter_();
		
		String[] ids 			= new String[]{"11111", "11111"};
		String[] subtypes 		= new String[]{"2", "2"};
		String[] enddates 		= new String[]{"9999-12-31", "9999-12-31"};
		String[] begindates 	= new String[]{"2005-03-08", "2005-03-08"};

		ResultList r_id 			= new TResultList();
		ResultList r_subtype 		= new TResultList();
		ResultList r_enddate 		= new TResultList();
		ResultList r_begindate 		= new TResultList();
		ResultList r_use 			= new TResultList();
		ResultList r_status			= new TResultList();
		ResultList r_statusMessage 	= new TResultList();
		
		try {
			inst.execute(
				ids,
				subtypes,
				enddates,
				begindates,
				r_id,
				r_subtype,
				r_enddate,
				r_begindate,
				r_use,
				r_status,
				r_statusMessage);
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("end");
	}

	@Init(description="") 
	public void init (
		 GlobalContainer container)  throws StreamTransformationException{		
	}

	@Cleanup 
	public void cleanup (
		 GlobalContainer container)  throws StreamTransformationException{
	}

	@LibraryMethod(title="filterRecords", description="", category="User-Defined", type=ExecutionType.ALL_VALUES_OF_CONTEXT) 
	public void filterRecords (
		@Argument(title="")  String[] ids,
		@Argument(title="")  String[] subtypes,
		@Argument(title="")  String[] enddates,
		@Argument(title="")  String[] begindates,
	
		ResultList r_id,
		ResultList r_subtype,
		ResultList r_enddate,
		ResultList r_begindate,
		ResultList r_use,
		ResultList r_status,
		ResultList r_statusMessage,
		Container container)  throws StreamTransformationException {
		
		try {
			this.trace = container.getTrace();
		} catch(Exception e) {
			
		}
		
		try {
			execute(
				ids,
				subtypes,
				enddates,
				begindates,
				
				r_id,
				r_subtype,
				r_enddate,
				r_begindate,
				
				r_use,
				r_status,
				r_statusMessage
			);
			
		} catch(Exception e) {
			try {
				container.getTrace().addWarning(e.toString());
			} catch(Exception e2) {
				
			}
		}
	}
}