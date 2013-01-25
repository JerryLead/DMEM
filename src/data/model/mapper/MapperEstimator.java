package data.model.mapper;

import java.util.ArrayList;
import java.util.List;

import profile.commons.configuration.Configuration;
import profile.commons.counters.MapperCounters;
import profile.task.mapper.Input;
import profile.task.mapper.Mapper;
import profile.task.mapper.MapperBuffer;
import profile.task.mapper.Merge;
import profile.task.mapper.Spill;

public class MapperEstimator {
	
	private Mapper estimatedMapper;
	private Configuration fConf;
	private Configuration newConf;
	
	public MapperEstimator(Configuration finishedJobConf, Configuration newConf) {
		this.fConf = finishedJobConf;
		this.newConf = newConf;
	}
	
	public Mapper estimateNewMapper(Mapper finishedMapper) {
		
		estimatedMapper = new Mapper();
		computeConcretePhase(finishedMapper);
		return estimatedMapper;
		
	}
	
	private void computeConcretePhase(Mapper finishedMapper) {
		int io_sort_mb = newConf.getIo_sort_mb();
		float io_sort_spill_percent = newConf.getIo_sort_spill_percent();
		float io_sort_record_percent = newConf.getIo_sort_record_percent();
		int mapred_reduce_tasks = newConf.getMapred_reduce_tasks();
		int min_num_spills_for_combine = newConf.getMin_num_spills_for_combine();
		boolean newCombine = newConf.getMapreduce_combine_class() != null ? true : false;
		long splitSize = finishedMapper.getInput().getSplitSize();
		
		long map_output_bytes = finishedMapper.getMapperCounters().getMap_output_bytes();
		long map_output_records = finishedMapper.getMapperCounters().getMap_output_records();

		Input eInput = new Input();
		eInput.setSplitSize(splitSize);
		//System.out.println("[Input] split.size = " + finishedMapper.getInput().getSplitSize());
		//System.out.println();
		
		MapperBuffer eBuffer = BufferModel.computeBufferLimit(io_sort_mb, io_sort_spill_percent, 
				io_sort_record_percent);
		//System.out.println();
		
		Spill eSpill = SpillModel.computeSpill(map_output_bytes, map_output_records, eBuffer, finishedMapper.getSpill());
		//System.out.println();
		
		Merge eMerge = MergeModel.computeMerge(newCombine, mapred_reduce_tasks, min_num_spills_for_combine, eSpill, finishedMapper, fConf);
		
		// set the estimated infos
		MapperCounters eCounters = estimatedMapper.getMapperCounters();
		MapperCounters fCounters = finishedMapper.getMapperCounters();
		
		eCounters.setFinalCountersItem("Map input records", fCounters.getMap_input_records());
		eCounters.setFinalCountersItem("Map input bytes", fCounters.getMap_input_bytes());
		eCounters.setFinalCountersItem("Map output records", fCounters.getMap_output_records());
		eCounters.setFinalCountersItem("Map output bytes", fCounters.getMap_output_bytes());
	
		eCounters.setSpill_combine_record_ratio(eSpill.getSpill_combine_record_ratio());
		eCounters.setSpill_combine_bytes_ratio(eSpill.getSpill_combine_bytes_ratio());
		eCounters.setMerge_combine_record_ratio(eMerge.getMerge_combine_record_ratio());
		eCounters.setMerge_combine_bytes_ratio(eMerge.getMerge_combine_bytes_ratio());
		
		estimatedMapper.setInput(eInput);
		estimatedMapper.setMapperBuffer(eBuffer);
		estimatedMapper.setSpill(eSpill);
		estimatedMapper.setMerge(eMerge);
		estimatedMapper.setConfiguration(newConf);	
		
	}

	public Mapper estimateNewMapper(List<Mapper> finishedMapperList, long newSplitSize) {
		estimatedMapper = new Mapper();
		
		List<Mapper> selectedMapperList = new ArrayList<Mapper>();
		long fSplitSize = fConf.getSplitSize();
		int fSplitMB = (int) (fSplitSize / (1024 * 1024));
		
		for(Mapper fMapper : finishedMapperList) {
			//if((double)fMapper.getMapperCounters().getMap_input_bytes() / fSplitSize < 1.001 || 
			//		(double)fSplitSize / fMapper.getMapperCounters().getMap_input_bytes() > 1.001)
			int fMapperInputBytesMB = (int) (fMapper.getMapperCounters().getMap_input_bytes() / (1024 * 1024));
			
			if(fMapperInputBytesMB >= fSplitMB || (fSplitMB - fMapperInputBytesMB) <= 5)
				selectedMapperList.add(fMapper);
		}
		
		if(selectedMapperList.isEmpty())
			for(Mapper fMapper : finishedMapperList)
				selectedMapperList.add(fMapper);
				
		computeConcretePhase(selectedMapperList, newSplitSize);
		
		return estimatedMapper;
	}

	private void computeConcretePhase(List<Mapper> finishedMapperList, long newSplitSize) {
		
		int io_sort_mb = newConf.getIo_sort_mb();
		float io_sort_spill_percent = newConf.getIo_sort_spill_percent();
		float io_sort_record_percent = newConf.getIo_sort_record_percent();
		int mapred_reduce_tasks = newConf.getMapred_reduce_tasks();
	
		long fmap_input_records = 0;
		long fmap_input_bytes = 0;
		long fmap_output_records = 0;
		long fmap_output_bytes = 0;
		
		for (Mapper fMapper : finishedMapperList) {
			fmap_input_records += fMapper.getMapperCounters().getMap_input_records();
			fmap_input_bytes += fMapper.getMapperCounters().getMap_input_bytes();
			fmap_output_records += fMapper.getMapperCounters().getMap_output_records();
			fmap_output_bytes += fMapper.getMapperCounters().getMap_output_bytes();
		}
		
		long map_input_records = 0;
		long map_output_records = 0;
		long map_output_bytes = 0;
		
		map_input_records = (long) ((double)fmap_input_records / fmap_input_bytes * newSplitSize);
		map_output_records = (long) ((double)fmap_output_records / fmap_input_bytes * newSplitSize);
		map_output_bytes = (long) ((double)fmap_output_bytes / fmap_input_bytes * newSplitSize);
			
		Input eInput = new Input();
		eInput.setSplitSize(newSplitSize);
		//System.out.println("[Input] split.size = " + newSplitSize);
		//System.out.println();
		
		MapperBuffer eBuffer = BufferModel.computeBufferLimit(io_sort_mb, io_sort_spill_percent, 
				io_sort_record_percent);
		//System.out.println();
		
		List<Spill> fSpillList = new ArrayList<Spill>();
		for(Mapper fMapper : finishedMapperList) 
			fSpillList.add(fMapper.getSpill());
		
		Spill eSpill = SpillModel.computeSpill(map_output_bytes, map_output_records, eBuffer, fSpillList);
		//System.out.println();
		
		List<Merge> fMergeList = new ArrayList<Merge>();
		for(Mapper fMapper : finishedMapperList) 
			fMergeList.add(fMapper.getMerge());
		
		Merge eMerge = MergeModel.computeMerge(mapred_reduce_tasks, eSpill, fSpillList, fMergeList, fConf.getMapred_reduce_tasks());
		
		// set the estiamted infos
		MapperCounters eCounters = estimatedMapper.getMapperCounters();
	
		estimatedMapper.setInput(eInput);
		estimatedMapper.setMapperBuffer(eBuffer);
		estimatedMapper.setSpill(eSpill);
		estimatedMapper.setMerge(eMerge);
		estimatedMapper.setConfiguration(newConf);	
		
		eCounters.setFinalCountersItem("Map input records", map_input_records);
		eCounters.setFinalCountersItem("Map input bytes", newSplitSize);
		eCounters.setFinalCountersItem("Map output records", map_output_records);
		eCounters.setFinalCountersItem("Map output bytes", map_output_bytes);
		
	}

}
