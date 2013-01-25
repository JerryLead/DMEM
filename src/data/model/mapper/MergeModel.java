package data.model.mapper;

import java.util.List;

import profile.commons.configuration.Configuration;
import profile.task.mapper.Mapper;
import profile.task.mapper.Merge;
import profile.task.mapper.MergeInfo;
import profile.task.mapper.Spill;
import profile.task.mapper.SpillInfo;

// sometimes without combine........
// if(eSpillInfoList.size() == 1) mergeInfo = spillInfo
//  minSpillsForCombine = job.getInt("min.num.spills.for.combine", 3);
public class MergeModel {
	public static Merge computeMerge(boolean newCombine, int mapred_reduce_tasks, int min_num_spills_for_combine, Spill eSpill, Mapper finishedMapper, Configuration fConf) {
		int fMin_num_spills_for_combine = fConf.getMin_num_spills_for_combine();
		boolean fCombine = fConf.getMapreduce_combine_class() != null ? true : false;
		boolean fMergeCombine = finishedMapper.getSpill().getSpillInfoList().size() < fMin_num_spills_for_combine ? false : true;
		
		Merge eMerge = new Merge();
		List<MergeInfo> finishedMergeInfoList = finishedMapper.getMerge().getMergeInfoList();
		
		List<SpillInfo> eSpillInfoList = eSpill.getSpillInfoList();
		
		boolean eMergeCombine = eSpillInfoList.size() < min_num_spills_for_combine ? false : true;
		
		long eTotalRecordsAfterCombine = 0;
		long eTotalRawLength = 0;
		long eTotalCompressedLength = 0;
		
		assert(mapred_reduce_tasks != 0);
		
		for(SpillInfo info : eSpillInfoList) {
			eTotalRecordsAfterCombine += info.getRecordsAfterCombine();
			eTotalRawLength += info.getRawLength();
			eTotalCompressedLength += info.getCompressedLength();
		}
		
		List<SpillInfo> finishedSpillInfoList = finishedMapper.getSpill().getSpillInfoList();
		long totalFinishedRecordsAfterCombine = 0;
		long totalFinishedRawLength = 0;
		long totalFinishedCompressedLength = 0;
		for(SpillInfo info: finishedSpillInfoList) {
			totalFinishedRecordsAfterCombine += info.getRecordsAfterCombine();
			totalFinishedRawLength += info.getRawLength();
			totalFinishedCompressedLength += info.getCompressedLength();
		}
		
		
		int segmentsNum = eSpillInfoList.size();
		int size = finishedMergeInfoList.size();
		
		long[] finishedRecordsBeforeMerge = new long[size];
		long[] finishedRawLengthBeforeMerge = new long[size];
		long[] finishedCompressedLengthBeforeMerge = new long[size];
		
		long[] finishedRecordsAfterMerge = new long[size];
		long[] finishedRawLengthAfterMerge = new long[size];
		long[] finishedCompressedLengthAfterMerge = new long[size];
		
		long totalFinishedRecordsBeforeMerge = 0;
		long totalFinishedRawLengthBeforeMerge = 0;
		long totalFinishedCompressedLengthBeforeMerge = 0;
		
		long totalFinishedRecordsAfterMerge = 0;
		long totalFinishedRawLengthAfterMerge = 0;
		long totalFinishedCompressedLengthAfterMerge = 0;

		for(int i = 0; i < size; i++) {
			MergeInfo info = finishedMergeInfoList.get(i);
			finishedRecordsBeforeMerge[i] = info.getRecordsBeforeMerge();
			finishedRawLengthBeforeMerge[i] = info.getRawLengthBeforeMerge();
			finishedCompressedLengthBeforeMerge[i] = info.getCompressedLengthBeforeMerge();
			finishedRecordsAfterMerge[i] = info.getRecordsAfterMerge();
			finishedRawLengthAfterMerge[i] = info.getRawLengthAfterMerge();
			finishedCompressedLengthAfterMerge[i] = info.getCompressedLengthAfterMerge();
			
			totalFinishedRecordsBeforeMerge += finishedRecordsBeforeMerge[i];
			totalFinishedRawLengthBeforeMerge += finishedRawLengthBeforeMerge[i];
			totalFinishedCompressedLengthBeforeMerge += finishedCompressedLengthBeforeMerge[i];
			
			totalFinishedRecordsAfterMerge += finishedRecordsAfterMerge[i];
			totalFinishedRawLengthAfterMerge += finishedRawLengthAfterMerge[i];
			totalFinishedCompressedLengthAfterMerge += finishedCompressedLengthAfterMerge[i];
		}
		
		double merge_combine_record_ratio = (double) totalFinishedRecordsAfterMerge / totalFinishedRecordsBeforeMerge;
		double merge_combine_bytes_ratio = (double) totalFinishedRawLengthAfterMerge / totalFinishedRawLengthBeforeMerge;
		eMerge.setMerge_combine_record_ratio(merge_combine_record_ratio); //need more consideration
		eMerge.setMerge_combine_bytes_ratio(merge_combine_bytes_ratio); // need more consideration
		
		double eMerge_combine_record_ratio;
		double eMerge_combine_bytes_ratio;
		
		if(newCombine == false || eMergeCombine == false) {
			eMerge_combine_record_ratio = 1;
			eMerge_combine_bytes_ratio = 1;
		}
		// combine in eMerge
		else if(fCombine == true && fMergeCombine == false){
			eMerge_combine_record_ratio = eSpill.getSpill_combine_record_ratio();
			eMerge_combine_bytes_ratio = eSpill.getSpill_combine_bytes_ratio();
		}
		// fCombine = true && fMergeCombine == true
		else {
			eMerge_combine_record_ratio = 0;
			eMerge_combine_bytes_ratio = 0;
		}
		
		// reducer number is not changed
		if(mapred_reduce_tasks == size) {
			for(int i = 0; i < size; i++) {
				long recordsBeforeMerge = (long) ((double)finishedRecordsBeforeMerge[i] / totalFinishedRecordsAfterCombine * eTotalRecordsAfterCombine);
				long rawLengthBeforeMerge = (long) ((double)finishedRawLengthBeforeMerge[i] / totalFinishedRawLength * eTotalRawLength);
				long compressedLengthBeforeMerge = (long) ((double)finishedCompressedLengthBeforeMerge[i] /totalFinishedCompressedLength * eTotalCompressedLength);
				
				long recordsAfterMerge;
				long rawLengthAfterMerge;
				long compressedLengthAfterMerge;
				
				if(eMerge_combine_record_ratio == 0 && eMerge_combine_bytes_ratio == 0) {
					recordsAfterMerge = (long) ((double)finishedRecordsAfterMerge[i] / finishedRecordsBeforeMerge[i] * recordsBeforeMerge);
					rawLengthAfterMerge = (long) ((double)finishedRawLengthAfterMerge[i] / finishedRawLengthBeforeMerge[i] * rawLengthBeforeMerge);
					compressedLengthAfterMerge = (long) ((double)finishedCompressedLengthAfterMerge[i] / finishedCompressedLengthBeforeMerge[i] * compressedLengthBeforeMerge);
				}
				else {
					recordsAfterMerge = (long) (eMerge_combine_record_ratio * recordsBeforeMerge);
					rawLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * rawLengthBeforeMerge);
					compressedLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * compressedLengthBeforeMerge);
				}
				
				
				MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
				newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);
	
				eMerge.addMergeInfo(newInfo);

				/*
				System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
						+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
			    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
				  		+ recordsBeforeMerge + ", "
				  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
				  		+ "RawLength = " + rawLengthAfterMerge + ", "
				  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
			    System.out.println();
			    */
			}
		}
		else {
			long recordsBeforeMerge = eTotalRecordsAfterCombine / mapred_reduce_tasks;
			long rawLengthBeforeMerge = eTotalRawLength / mapred_reduce_tasks;
			long compressedLengthBeforeMerge = eTotalCompressedLength / mapred_reduce_tasks;
			
			long recordsAfterMerge;
			long rawLengthAfterMerge;
			long compressedLengthAfterMerge;
			
			if(eMerge_combine_record_ratio == 0 && eMerge_combine_bytes_ratio == 0) {		
				recordsAfterMerge = (long) ((double)totalFinishedRecordsAfterMerge / totalFinishedRecordsBeforeMerge * recordsBeforeMerge);
				rawLengthAfterMerge = (long) ((double)totalFinishedRawLengthAfterMerge / totalFinishedRawLengthBeforeMerge * rawLengthBeforeMerge);
				compressedLengthAfterMerge = (long) ((double)totalFinishedCompressedLengthAfterMerge / totalFinishedCompressedLengthBeforeMerge * compressedLengthBeforeMerge);
			}
			else {
				recordsAfterMerge = (long) (eMerge_combine_record_ratio * recordsBeforeMerge);
				rawLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * rawLengthBeforeMerge);
				compressedLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * compressedLengthBeforeMerge);
			}
			
			for(int i = 0; i < mapred_reduce_tasks; i++) {
				MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
				newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);		
				eMerge.addMergeInfo(newInfo);
				
				/*
				System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
						+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
			    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
				  		+ recordsBeforeMerge + ", "
				  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
				  		+ "RawLength = " + rawLengthAfterMerge + ", "
				  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
			    System.out.println();
			    */
			}
		}
		return eMerge;

	}

	public static Merge computeMerge(int mapred_reduce_tasks, Spill eSpill, List<Spill> fSpillList, List<Merge> fMergeList, int fReducerNum) {
		Merge eMerge = new Merge();
		
		List<SpillInfo> eSpillInfoList = eSpill.getSpillInfoList();
		long eTotalRecordsAfterCombine = 0;
		long eTotalRawLength = 0;
		long eTotalCompressedLength = 0;
		
		assert(mapred_reduce_tasks != 0);
		
		for(SpillInfo info : eSpillInfoList) {
			eTotalRecordsAfterCombine += info.getRecordsAfterCombine();
			eTotalRawLength += info.getRawLength();
			eTotalCompressedLength += info.getCompressedLength();
		}
		
		long totalFinishedRecordsAfterCombine = 0;
		long totalFinishedRawLength = 0;
		long totalFinishedCompressedLength = 0;
		
		for(Spill fSpill : fSpillList) {
			for(SpillInfo info: fSpill.getSpillInfoList()) {
				totalFinishedRecordsAfterCombine += info.getRecordsAfterCombine();
				totalFinishedRawLength += info.getRawLength();
				totalFinishedCompressedLength += info.getCompressedLength();
			}
		}
		
		int segmentsNum = eSpillInfoList.size();
		int size = fReducerNum;
		
		long[] finishedRecordsBeforeMerge = new long[size];
		long[] finishedRawLengthBeforeMerge = new long[size];
		long[] finishedCompressedLengthBeforeMerge = new long[size];
		
		long[] finishedRecordsAfterMerge = new long[size];
		long[] finishedRawLengthAfterMerge = new long[size];
		long[] finishedCompressedLengthAfterMerge = new long[size];
		
		for(int i = 0; i < size; i++) {
			finishedRecordsBeforeMerge[i] = 0;
			finishedRawLengthBeforeMerge[i] = 0;
			finishedCompressedLengthBeforeMerge[i] = 0;
			finishedRecordsAfterMerge[i] = 0;
			finishedRawLengthAfterMerge[i] = 0;
			finishedCompressedLengthAfterMerge[i] = 0;
		}
		
		long totalFinishedRecordsBeforeMerge = 0;
		long totalFinishedRawLengthBeforeMerge = 0;
		long totalFinishedCompressedLengthBeforeMerge = 0;
		
		long totalFinishedRecordsAfterMerge = 0;
		long totalFinishedRawLengthAfterMerge = 0;
		long totalFinishedCompressedLengthAfterMerge = 0;

		for(Merge fMerge : fMergeList) {
			assert(fMerge.getMergeInfoList().size() == size);
			
			for(int i = 0; i < size; i++) {
				MergeInfo info = fMerge.getMergeInfoList().get(i);
				finishedRecordsBeforeMerge[i] += info.getRecordsBeforeMerge();
				finishedRawLengthBeforeMerge[i] += info.getRawLengthBeforeMerge();
				finishedCompressedLengthBeforeMerge[i] += info.getCompressedLengthBeforeMerge();
				finishedRecordsAfterMerge[i] += info.getRecordsAfterMerge();
				finishedRawLengthAfterMerge[i] += info.getRawLengthAfterMerge();
				finishedCompressedLengthAfterMerge[i] += info.getCompressedLengthAfterMerge();
				
				totalFinishedRecordsBeforeMerge += info.getRecordsBeforeMerge();
				totalFinishedRawLengthBeforeMerge += info.getRawLengthBeforeMerge();
				totalFinishedCompressedLengthBeforeMerge += info.getCompressedLengthBeforeMerge();
				
				totalFinishedRecordsAfterMerge += info.getRecordsAfterMerge();
				totalFinishedRawLengthAfterMerge += info.getRawLengthAfterMerge();
				totalFinishedCompressedLengthAfterMerge += info.getCompressedLengthAfterMerge();
			}
		}
		
		
		// reducer number is not changed
		if(mapred_reduce_tasks == size) {
			for(int i = 0; i < size; i++) {
				long recordsBeforeMerge = (long) ((double)finishedRecordsBeforeMerge[i] / totalFinishedRecordsAfterCombine * eTotalRecordsAfterCombine);
				long rawLengthBeforeMerge = (long) ((double)finishedRawLengthBeforeMerge[i] / totalFinishedRawLength * eTotalRawLength);
				long compressedLengthBeforeMerge = (long) ((double)finishedCompressedLengthBeforeMerge[i] /totalFinishedCompressedLength * eTotalCompressedLength);
				
				long recordsAfterMerge = (long) ((double)finishedRecordsAfterMerge[i] / finishedRecordsBeforeMerge[i] * recordsBeforeMerge);
				long rawLengthAfterMerge = (long) ((double)finishedRawLengthAfterMerge[i] / finishedRawLengthBeforeMerge[i] * rawLengthBeforeMerge);
				long compressedLengthAfterMerge = (long) ((double)finishedCompressedLengthAfterMerge[i] / finishedCompressedLengthBeforeMerge[i] * compressedLengthBeforeMerge);
				
				MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
				newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);
	
				eMerge.addMergeInfo(newInfo);

				/*
				System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
						+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
			    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
				  		+ recordsBeforeMerge + ", "
				  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
				  		+ "RawLength = " + rawLengthAfterMerge + ", "
				  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
			    System.out.println();
			    */
			}
		}
		else {
			long recordsBeforeMerge = eTotalRecordsAfterCombine / mapred_reduce_tasks;
			long rawLengthBeforeMerge = eTotalRawLength / mapred_reduce_tasks;
			long compressedLengthBeforeMerge = eTotalCompressedLength / mapred_reduce_tasks;
			
			long recordsAfterMerge = (long) ((double)totalFinishedRecordsAfterMerge / totalFinishedRecordsBeforeMerge * recordsBeforeMerge);
			long rawLengthAfterMerge = (long) ((double)totalFinishedRawLengthAfterMerge / totalFinishedRawLengthBeforeMerge * rawLengthBeforeMerge);
			long compressedLengthAfterMerge = (long) ((double)totalFinishedCompressedLengthAfterMerge / totalFinishedCompressedLengthBeforeMerge * compressedLengthBeforeMerge);
			
			for(int i = 0; i < mapred_reduce_tasks; i++) {
				MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
				newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);		
				eMerge.addMergeInfo(newInfo);
				
				/*
				System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
						+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
			    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
				  		+ recordsBeforeMerge + ", "
				  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
				  		+ "RawLength = " + rawLengthAfterMerge + ", "
				  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
			    System.out.println();
			    */
			}
		}
		return eMerge;
	}
}