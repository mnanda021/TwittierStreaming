package com.adbms.tweets;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PiePlot3D;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.util.Rotation;

public class TwitterChart {

	private static final long serialVersionUID = 1L;

	public TwitterChart(String applicationTitle, String chartTitle, Map<Double, List<String>> map) throws Exception {
		// This will create the dataset 
		PieDataset dataset = createDataset( map );
		// based on the dataset we create the chart
		JFreeChart chart = createChart(dataset, chartTitle);
		ChartUtilities.saveChartAsJPEG(new File("c:\\project\\Twitter\\" +chartTitle+ ".jpg"), chart, 500, 500);
	}

	 
	/**
	 * Creates a sample dataset 
	 */

	private  PieDataset createDataset( Map<Double, List<String>> top10 ) {
		DefaultPieDataset result = new DefaultPieDataset();
		
		System.out.println("****RESULTS***");
		int count = 0;
		for( double val: top10.keySet() ) {
			result.setValue(top10.get(val).toString(), val);
			count++;
			if( count == 7 ) {
				break;
			}
		}
		return result;

	}


	/**
	 * Creates a chart
	 */

	private JFreeChart createChart(PieDataset dataset, String title) {

		JFreeChart chart = ChartFactory.createPieChart3D(title,          // chart title
				dataset,                // data
				true,                   // include legend
				true,
				false);

		PiePlot3D plot = (PiePlot3D) chart.getPlot();
		plot.setStartAngle(290);
		plot.setDirection(Rotation.CLOCKWISE);
		plot.setForegroundAlpha(0.5f);
		return chart;

	}	

}
