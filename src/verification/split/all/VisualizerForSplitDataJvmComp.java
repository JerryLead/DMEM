package verification.split.all;


import verification.gnuplot.DiffDataflowPlot;
import verification.gnuplot.DiffJvmCostPlot;
import verification.split.gnuplot.SplitDiffDataflowPlot;
import verification.split.gnuplot.SplitDiffJvmCostPlot;
import verification.split.jvmcost.SplitJvmCostComparator;
import verification.split.dataflow.SplitDataflowComparator;

public class VisualizerForSplitDataJvmComp {

	public static void main(String[] args) {
		//String jobName = "BuildCompIndex-m36-r18-256MB";
		//String jobName = "BuildCompIndex-m36-r18-256MB";
		//String jobName = "uservisits_aggre-pig-256MB";
		//String jobName = "BuildCompIndex-m36-r18";
		//String jobName = "Wiki-m36-r18";
		//String jobName = "BigTwitterInDegreeCount";
		//String jobName = "big-uservisits_aggre-pig-256MB";
		//String jobName = "BigTwitterBiDirectEdgeCount";
		//String jobName = "BigTeraSort";
		//String jobName = "BigTeraSort-36GB";
		//String jobName = "SampleTeraSort-1G";
		//String jobName = "Big-uservisits_aggre-pig-50G";
		String jobName = "BigBuildInvertedIndex";
		
		String baseDir = "/home/xulijie/MR-MEM/BigExperiments/";
		//String baseDir = "/home/xulijie/MR-MEM/SampleExperiments/";
		//String baseDir = "/home/xulijie/MR-MEM/Test/";

		//int splitMB[] = {64, 128, 256}; //set the split size to filter the finished mappers
		
		
		SplitDataflowComparator dataComp = new SplitDataflowComparator(jobName, baseDir);
		dataComp.compareDataflow();
		
		SplitJvmCostComparator jvmComp = new SplitJvmCostComparator(jobName, baseDir);
		jvmComp.compareJvmCost();
		
		SplitDiffDataflowPlot dataPlot = new SplitDiffDataflowPlot(jobName, baseDir);
		dataPlot.visualize();
		
		SplitDiffJvmCostPlot jvmPlot = new SplitDiffJvmCostPlot(jobName, baseDir);
		jvmPlot.visualize();
		
		
		
		System.out.println("Finished!");	
	}
}
