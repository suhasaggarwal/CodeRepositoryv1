package util;

public class CountingSort {

	
	
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
       
		int input[]=new int[21];
		int output[]=new int[21];
		
		for(int r=0;r<20;r++)
		{
			input[r]= (int)((Math.random() * ( 9 - 1 )) + 1); 
			
		}
    	   
    	for(int t=0;t<20;t++){
    		
    		System.out.println(input[t]);
    	}
		
	  int a[] = new int[10];
	  int i,j;
	  
	  for(int k=1;k<10;k++){
		  a[k]=0;
		  
	  }
	
	 for(int l=0;l<20;l++){
		 a[input[l]]=a[input[l]]+1;
		 
	 }
	
      for(int m=1;m<10;m++)	
      {
    	  
    	  a[m]=a[m]+a[m-1];
      }
	
	
	  for(int p=20;p>=0;p--){
		
		  output[a[input[p]]] = input[p];
		  a[input[p]] = a[input[p]] -1;
		  
	  }
	  
	  
	  
	  System.out.println("Sorted");
      
	  for(int t1=0;t1<20;t1++){
  		
  		System.out.println(output[t1]);
  	}
	
      
	
	}

}
