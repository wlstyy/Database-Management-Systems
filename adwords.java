/**
 * DBMS PROJECT
 * @author taoyu
 * version 1
 */
import java.io.File;
import java.util.Scanner;
public class adwords {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		File file=new File("system.in");
		Scanner input=new Scanner(file);
		String username=input.nextLine();
		String[] tokens=username.split("\\s");
		username=tokens[tokens.length-1];
		String password=input.nextLine();
		tokens=password.split("\\s");
		password=tokens[tokens.length-1];
		String temp=input.nextLine();
		tokens=temp.split("\\s");
		String v1=tokens[tokens.length-1];
		temp=input.nextLine();
		tokens=temp.split("\\s");
		String v2=tokens[tokens.length-1];
		temp=input.nextLine();
		tokens=temp.split("\\s");
		String v3=tokens[tokens.length-1];
		temp=input.nextLine();
		tokens=temp.split("\\s");
		String v4=tokens[tokens.length-1];
		temp=input.nextLine();
		tokens=temp.split("\\s");
		String v5=tokens[tokens.length-1];
		temp=input.nextLine();
		tokens=temp.split("\\s");
		String v6=tokens[tokens.length-1];
		input.close();
		String commandbase="sqlplus "+username+"@orcl/"+password+" ";
		String command1=commandbase+"@createtables.sql";
		Process p = Runtime.getRuntime().exec(command1);
		p.waitFor();
		String loadbase="sqlldr "+username+"@orcl/"+password+" ";
		String command2=loadbase+"control='Keywords.ctl' parallel=true direct=true";
		p=Runtime.getRuntime().exec(command2);
        	p.waitFor();
        	String command3=loadbase+"control='Advertisers.ctl' parallel=true direct=true";
        	p=Runtime.getRuntime().exec(command3);
        	p.waitFor();
        	String command4=loadbase+"control='Queries.ctl' parallel=true direct=true";
        	p=Runtime.getRuntime().exec(command4);
       		p.waitFor();
        	String command5=commandbase+"@adwords.sql "+v1+" "+v2+" "+v3+" "+v4+" "+v5+" "+v6;
        	p=Runtime.getRuntime().exec(command5);
		p.waitFor();
	}

}
