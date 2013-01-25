package data.model.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import profile.task.mapper.Mapper;
import profile.task.mapper.MapperBuffer;
import profile.task.mapper.Spill;
import profile.task.mapper.SpillInfo;


public class SpillModel {
	
	private static Spill generateNewSpill(long map_output_bytes, long map_output_records, MapperBuffer eBuffer) {
		Spill newSpill = new Spill();
		int spillBytesCounts =  (int) Math.ceil(((double) map_output_bytes / eBuffer.getSoftBufferLimit()));
		int spillRecordsCounts = (int) Math.ceil(((double) map_output_records / eBuffer.getSoftRecordLimit()));
		
		String reason = "record";
		int spillCounts = spillRecordsCounts;
		long recordsBeforeCombine = eBuffer.getSoftRecordLimit();
		// (total bytes / total records) * recordsInOneSpill
		long bytesBeforeSpill = (long)((double) map_output_bytes / map_output_records * recordsBeforeCombine);
		
		if(spillBytesCounts > spillRecordsCounts) {
			spillCounts = spillBytesCounts;
			reason = "buffer";
			
			bytesBeforeSpill = eBuffer.getSoftBufferLimit();
			recordsBeforeCombine = (long) (bytesBeforeSpill / ((double) map_output_bytes / map_output_records));
		}
		
		long flushRecords = map_output_records - (spillCounts - 1) * recordsBeforeCombine;
		long flushBytes = map_output_bytes - (spillCounts - 1) * bytesBeforeSpill;
		
		
		for(int i = 0; i < spillCounts - 1; i++) {
			SpillInfo spillInfo = new SpillInfo(reason, recordsBeforeCombine, bytesBeforeSpill);
			newSpill.addSpillInfo(spillInfo);
		}
		
		SpillInfo flushInfo = new SpillInfo("flush", flushRecords, flushBytes);
		newSpill.addSpillInfo(flushInfo);
		
		return newSpill;
	}
	
	//suppose io_sort_mb, io_sort_spill_percent, io_sort_record_percent have been set.
	public static Spill computeSpill(long map_output_bytes, long map_output_records, MapperBuffer eBuffer, Spill oSpill) {
		Spill newSpill = generateNewSpill(map_output_bytes, map_output_records, eBuffer);	
		refineSpillInfo(newSpill, oSpill);
		return newSpill;
	}	
	
	public static void refineSpillInfo(Spill newSpill, Spill oSpill) {
		//get SpillInfo list from finished map task
		List<SpillInfo> finishedSpillInfoList = oSpill.getSpillInfoList(); 
		
		List<SpillInfo> newSpillInfoList = newSpill.getSpillInfoList();
		
		int size = finishedSpillInfoList.size();
		if(size > 1) {
			SpillInfo lastInfo = finishedSpillInfoList.get(size - 1);
			double lastRatio = (double)lastInfo.getRecordsAfterCombine() / lastInfo.getRecordsBeforeCombine();
			
			SpillInfo last2ndInfo = finishedSpillInfoList.get(size - 2);
			double last2ndRatio = (double)last2ndInfo.getRecordsAfterCombine() / last2ndInfo.getRecordsBeforeCombine();
			
			if(lastRatio / last2ndRatio > 1.1 || last2ndRatio / lastRatio > 1.1) //discard the last finished spill info
				size = size - 1;		
		}
		
		long fRecordsBeforeCombine = 0;
		long fBytesBeforeCombine = 0;
		
		long fRecordsAfterCombine = 0;
		long fRawLength = 0;
		long fCompressedLength = 0;
		
		
		for(int i = 0; i < size; i++) {
			SpillInfo info = finishedSpillInfoList.get(i);
			fRecordsBeforeCombine += info.getRecordsBeforeCombine();
			fBytesBeforeCombine += info.getBytesBeforeSpill();
			fRecordsAfterCombine += info.getRecordsAfterCombine();
			fRawLength += info.getRawLength();
			fCompressedLength += info.getCompressedLength();
		}
		
		double spill_combine_record_ratio = (double) fRecordsAfterCombine / fRecordsBeforeCombine;
		double spill_combine_bytes_ratio = (double) fRawLength / fBytesBeforeCombine;
		
		newSpill.setSpill_combine_record_ratio(spill_combine_record_ratio);
		newSpill.setSpill_combine_bytes_ratio(spill_combine_bytes_ratio);
		
		for(SpillInfo newInfo : newSpillInfoList) {
			long newRecordsBeforeCombine = newInfo.getRecordsBeforeCombine();
			long newBytesBeforeCombine = newInfo.getBytesBeforeSpill();
						
			long newRecordsAfterCombine = (long) ((double)newRecordsBeforeCombine / fRecordsBeforeCombine * fRecordsAfterCombine);
			long newRawLength = (long) ((double)newBytesBeforeCombine / fBytesBeforeCombine * fRawLength);
			long newCompressedLength = (long) ((double)newBytesBeforeCombine / fBytesBeforeCombine * fCompressedLength);
			
			newInfo.setAfterSpillInfo(newRecordsAfterCombine, newRawLength, newCompressedLength);
			
			/*
			System.out.println("[Spill] <RecordsBeforeCombine = " + newInfo.getRecordsBeforeCombine() + ", "
        			+ "BytesBeforeSpill = " + newInfo.getBytesBeforeSpill() + ", "
        			+ "RecordAfterCombine = " + newRecordsAfterCombine + ", " 
        			+ "RawLength = " + newRawLength + ", CompressedLength = " + newCompressedLength + ">");
        	*/
		}
		
		
	}
	
	/*
	public static void refineSpillInfo(Spill newSpill, Spill oSpill) {
		//get SpillInfo list from finished map task
		List<SpillInfo> finishedSpillInfoList = oSpill.getSpillInfoList(); 
		
		List<SpillInfo> newSpillInfoList = newSpill.getSpillInfoList();
		
		int size = finishedSpillInfoList.size();
		if(size > 1) {
			SpillInfo lastInfo = finishedSpillInfoList.get(size - 1);
			double lastRatio = (double)lastInfo.getRecordsAfterCombine() / lastInfo.getRecordsBeforeCombine();
			
			SpillInfo last2ndInfo = finishedSpillInfoList.get(size - 2);
			double last2ndRatio = (double)last2ndInfo.getRecordsAfterCombine() / last2ndInfo.getRecordsBeforeCombine();
			
			if(lastRatio / last2ndRatio > 1.1 || last2ndRatio / lastRatio > 1.1) //discard the last finished spill info
				size = size - 1;
			
			
		}
		long[] fRecordsBeforeCombine = new long[size];
		long[] fBytesBeforeCombine = new long[size];
		
		long[] fRecordsAfterCombine = new long[size];
		long[] fRawLength = new long[size];
		long[] fCompressedLength = new long[size];
		
		long[] fIncrementalRecords = new long[size];
		
		for(int i = 0; i < size; i++) {
			SpillInfo info = finishedSpillInfoList.get(i);
			fRecordsBeforeCombine[i] = info.getRecordsBeforeCombine();
			fBytesBeforeCombine[i] = info.getBytesBeforeSpill();
			fRecordsAfterCombine[i] = info.getRecordsAfterCombine();
			fRawLength[i] = info.getRawLength();
			fCompressedLength[i] = info.getCompressedLength();
			
			if(i == 0)
				fIncrementalRecords[i] = fRecordsBeforeCombine[i];
			else
				fIncrementalRecords[i] = fRecordsBeforeCombine[i] + fIncrementalRecords[i - 1];
		}
		
		int index = 0;
		int newIndex = 0;
		long newIncrementalBeforeCombine = 0;
		
		for(SpillInfo newInfo : newSpillInfoList) {
			long newRecordsBeforeCombine = newInfo.getRecordsBeforeCombine();
			long newBytesBeforeCombine = newInfo.getBytesBeforeSpill();
			
			newIncrementalBeforeCombine += newRecordsBeforeCombine;
			
			newIndex = index;
			while((double)newIncrementalBeforeCombine / fIncrementalRecords[newIndex] > 1.01 && newIndex < size - 1) 
				newIndex++;
				

			long incRecordsBeforeCombine = 0;
			long incBytesBeforeCombine = 0;
			
			long incRecordsAfterCombine = 0;
			long incRawLength = 0;
			long incCompressedLength = 0;
			
			for(int i = index; i <= newIndex; i++) {
				incRecordsBeforeCombine += fRecordsBeforeCombine[i];
				incBytesBeforeCombine += fBytesBeforeCombine[i];
				
				incRecordsAfterCombine += fRecordsAfterCombine[i];
				incRawLength += fRawLength[i];
				incCompressedLength += fCompressedLength[i];
			}
						
			long newRecordsAfterCombine = (long) ((double)newRecordsBeforeCombine / incRecordsBeforeCombine * incRecordsAfterCombine);
			long newRawLength = (long) ((double)newBytesBeforeCombine / incBytesBeforeCombine * incRawLength);
			long newCompressedLength = (long) ((double)newBytesBeforeCombine / incBytesBeforeCombine * incCompressedLength );
			
			newInfo.setAfterSpillInfo(newRecordsAfterCombine, newRawLength, newCompressedLength);
			
			
			//System.out.println("[Spill] <RecordsBeforeCombine = " + newInfo.getRecordsBeforeCombine() + ", "
        	//		+ "BytesBeforeSpill = " + newInfo.getBytesBeforeSpill() + ", "
        	//		+ "RecordAfterCombine = " + newRecordsAfterCombine + ", " 
        	//		+ "RawLength = " + newRawLength + ", CompressedLength = " + newCompressedLength + ">");
        	
			index = newIndex;		
		}
	}
	*/

	public static Spill computeSpill(long map_output_bytes, long map_output_records, MapperBuffer eBuffer,
			List<Spill> fSpillList) {
		
		Spill newSpill = generateNewSpill(map_output_bytes, map_output_records, eBuffer);	
		refineSpillInfo(newSpill, fSpillList);
		return newSpill;
	}

	private static void refineSpillInfo(Spill newSpill, List<Spill> fSpillList) {
		//get SpillInfo list from finished map task
		List<SpillInfo> allFSpillInfoList = new ArrayList<SpillInfo>();
		
		for(Spill oSpill : fSpillList)  {
			List<SpillInfo> finishedSpillInfoList = oSpill.getSpillInfoList(); 
			int size = finishedSpillInfoList.size();
			if(size > 1) {
				SpillInfo lastInfo = finishedSpillInfoList.get(size - 1);
				double lastRatio = (double)lastInfo.getRecordsAfterCombine() / lastInfo.getRecordsBeforeCombine();
				
				SpillInfo last2ndInfo = finishedSpillInfoList.get(size - 2);
				double last2ndRatio = (double)last2ndInfo.getRecordsAfterCombine() / last2ndInfo.getRecordsBeforeCombine();
				
				if(lastRatio / last2ndRatio > 1.1 || last2ndRatio / lastRatio > 1.1) //discard the last finished spill info
					size = size - 1;
				
			}
			for(int i = 0; i < size; i++) 
				allFSpillInfoList.add(finishedSpillInfoList.get(i));
		}
		
		
		List<SpillInfo> newSpillInfoList = newSpill.getSpillInfoList();
		
		int size = allFSpillInfoList.size();
		long fRecordsBeforeCombine = 0;
		long fBytesBeforeCombine = 0;
		
		long fRecordsAfterCombine = 0;
		long fRawLength = 0;
		long fCompressedLength = 0;
		
		for(int i = 0; i < size; i++) {
			SpillInfo info = allFSpillInfoList.get(i);
			fRecordsBeforeCombine += info.getRecordsBeforeCombine();
			fBytesBeforeCombine += info.getBytesBeforeSpill();
			fRecordsAfterCombine += info.getRecordsAfterCombine();
			fRawLength += info.getRawLength();
			fCompressedLength += info.getCompressedLength();
		}	
		
		//use the average records/bytes i/o ratio to estimate the new spill
		for(SpillInfo newInfo : newSpillInfoList) {
			long newRecordsBeforeCombine = newInfo.getRecordsBeforeCombine();
			long newBytesBeforeCombine = newInfo.getBytesBeforeSpill();
					
			long newRecordsAfterCombine = (long) ((double)newRecordsBeforeCombine / fRecordsBeforeCombine * fRecordsAfterCombine);
			long newRawLength = (long) ((double)newBytesBeforeCombine / fBytesBeforeCombine * fRawLength);
			long newCompressedLength = (long) ((double)newBytesBeforeCombine / fBytesBeforeCombine * fCompressedLength);
			
			newInfo.setAfterSpillInfo(newRecordsAfterCombine, newRawLength, newCompressedLength);
				
			/*
			System.out.println("[Spill] <RecordsBeforeCombine = " + newInfo.getRecordsBeforeCombine() + ", "
        			+ "BytesBeforeSpill = " + newInfo.getBytesBeforeSpill() + ", "
        			+ "RecordAfterCombine = " + newRecordsAfterCombine + ", " 
        			+ "RawLength = " + newRawLength + ", CompressedLength = " + newCompressedLength + ">");
        	*/
		}
	}
}
