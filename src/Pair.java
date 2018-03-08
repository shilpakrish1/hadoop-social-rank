//Simple pair class to hold a vertex and its page rank for the finish stage
public class Pair implements Comparable<Object>{
	String key;
	Double probability; 
	
	public Pair (String s, Double i) {
		this.key = s;
		this.probability = i;
	}
	
	public Double getProb() {
		return probability;
	}
	
	public String getString() {
		return key;
	}
	
	//Implements comparator to enable use of a maximum priority queue
	public int compareTo(Object other) {
		Pair p = (Pair) other;
		return probability.compareTo(p.getProb());
	}


}
