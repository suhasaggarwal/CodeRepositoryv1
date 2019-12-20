package com.spring.util;

import java.io.File;  
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;  
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;  
import javax.mail.internet.MimeMultipart;

public class SendEmail{  

 
public static String sendEmail (String email, String token){	

   /*
	// Recipient's email ID needs to be mentioned.
    String to = email;

    // Sender's email ID needs to be mentioned
    String from = "no-reply@mail.cuberootmail.com";

    // Assuming you are sending email from localhost
    String host = "122.248.250.128";

    // Get system properties
    Properties properties = System.getProperties();

    // Setup mail server
    properties.setProperty("mail.smtp.host", host);

    // Get the default Session object.
    Session session = Session.getDefaultInstance(properties);

 //   String unique_token = UUIDGenerator.generate();
    String status = "false";
   
    
    try{
       // Create a default MimeMessage object.
       MimeMessage message = new MimeMessage(session);

       // Set From: header field of the header.
       message.setFrom(new InternetAddress(from));

       // Set To: header field of the header.
       message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

       // Set Subject: header field
       message.setSubject("!");

       // Send the actual HTML message, as big as you like
       message.setContent("<html><body>Hi,<br/><br/><a href='http://http://54.84.60.73:8080/usermod/ConfirmRegistration/"+token+"/'> Click here</a> to confirm Registration</body></html>", "text/html" );

       // Send message
       Transport.send(message);
       
       status = "true";
       
       System.out.println("Sent message successfully....");
    }catch (MessagingException mex) {
       mex.printStackTrace();
    }


    return status;


/*
	Properties props = System.getProperties();
    props.put("mail.smtp.starttls.enable", true); // added this line
    props.put("mail.smtp.host", "smtp.gmail.com");
    props.put("mail.smtp.user", "suhaas.cuberoot");
    props.put("mail.smtp.password", "aggarwal456");
    props.put("mail.smtp.port", "587");
    props.put("mail.smtp.auth", true);



    Session session = Session.getInstance(props,null);
    MimeMessage message = new MimeMessage(session);

    System.out.println("Port: "+session.getProperty("mail.smtp.port"));

    // Create the email addresses involved
    try {
        InternetAddress from = new InternetAddress("username");
        message.setSubject("Yes we can");
        message.setFrom(from);
        message.addRecipients(Message.RecipientType.TO, InternetAddress.parse("receivermail"));

        // Create a multi-part to combine the parts
        Multipart multipart = new MimeMultipart("alternative");

        // Create your text message part
        BodyPart messageBodyPart = new MimeBodyPart();
        messageBodyPart.setText("some text to send");

        // Add the text part to the multipart
        multipart.addBodyPart(messageBodyPart);

        // Create the html part
        messageBodyPart = new MimeBodyPart();
        String htmlMessage = "Our html text";
        messageBodyPart.setContent(htmlMessage, "text/html");


        // Add html part to multi part
        multipart.addBodyPart(messageBodyPart);

        // Associate multi-part with message
        message.setContent(multipart);

        // Send message
        Transport transport = session.getTransport("smtp");
        transport.connect("smtp.gmail.com", "username", "password");
        System.out.println("Transport: "+transport.toString());
        transport.sendMessage(message, message.getAllRecipients());


    } catch (AddressException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    } catch (MessagingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
}
*/	
	 String status;
	
	
	 String to = email;//change accordingly

     // Sender's email ID needs to be mentioned
     String from = "suhaas.aggarwal@cuberoot.co";//change accordingly
     final String username = "suhaas.aggarwal@cuberoot.co";//change accordingly
     final String password = "dfg678suh";//change accordingly

     // Assuming you are sending email through relay.jangosmtp.net
     String host = "mail.cuberoot.co";

     Properties props = new Properties();
     props.put("mail.smtp.auth", "true");
     props.put("mail.smtp.starttls.enable", "true");
     props.put("mail.smtp.host", host);
     props.put("mail.smtp.port", "587");
     props.put("mail.smtp.ssl.trust", "mail.cuberoot.co");


     
     
     // Get the Session object.
     Session session = Session.getInstance(props,
     new javax.mail.Authenticator() {
        protected PasswordAuthentication getPasswordAuthentication() {
           return new PasswordAuthentication(username, password);
        }
     });

     try {
        // Create a default MimeMessage object.
        Message message = new MimeMessage(session);

        // Set From: header field of the header.
        message.setFrom(new InternetAddress(from));

        // Set To: header field of the header.
        message.setRecipients(Message.RecipientType.TO,
        InternetAddress.parse(to));

        // Set Subject: header field
        message.setSubject("Registration Confirmation Mail ");

        // Now set the actual message
        message.setContent("<html><body>Hi,<br/><br/><a href='http://54.84.60.73:8080/usermod/ConfirmRegistration/"+token+"/'> Click here</a> to confirm Registration</body></html>", "text/html" );

        // Send message
        Transport.send(message);
        
        status = "true";

        System.out.println("Sent message successfully....");

       return status;
     
     } catch (MessagingException e) {
           throw new RuntimeException(e);
     }	
	
	
	
	

}




public static String sendEmailForgotPassword (String email, String token){	
	
	/*
	
	// Recipient's email ID needs to be mentioned.
    String to = email;

    // Sender's email ID needs to be mentioned
    String from = "no-reply@mail.cuberootmail.com";

    // Assuming you are sending email from localhost
    String host = "54.84.60.73";

    // Get system properties
    Properties properties = System.getProperties();

    // Setup mail server
    properties.setProperty("mail.smtp.host", host);

    // Get the default Session object.
    Session session = Session.getDefaultInstance(properties);

    String status = "false";
    
    //   String unique_token = UUIDGenerator.generate();
    
    try{
       // Create a default MimeMessage object.
       MimeMessage message = new MimeMessage(session);

       // Set From: header field of the header.
       message.setFrom(new InternetAddress(from));

       // Set To: header field of the header.
       message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

       // Set Subject: header field
       message.setSubject("!");

       // Send the actual HTML message, as big as you like
       message.setContent("<html><body>Hi,<br/><br/>"+"Your new Password is "+token+"</body></html>", "text/html" );

       // Send message
       Transport.send(message);
      
       status = "true";
       
       System.out.println("Sent message successfully....");
    }catch (MessagingException mex) {
       mex.printStackTrace();
    }


    return status;
  */


	 String status;
		
		
	 String to = email;//change accordingly

	 String from = "suhaas.aggarwal@cuberoot.co";//change accordingly
     final String username = "suhaas.aggarwal@cuberoot.co";//change accordingly
     final String password = "dfg678suh";//change accordingly

     // Assuming you are sending email through relay.jangosmtp.net
     String host = "mail.cuberoot.co";

     Properties props = new Properties();
     props.put("mail.smtp.auth", "true");
     props.put("mail.smtp.starttls.enable", "true");
     props.put("mail.smtp.host", host);
     props.put("mail.smtp.port", "587");
     props.put("mail.smtp.ssl.trust", "mail.cuberoot.co");


     
     
     // Get the Session object.
     Session session = Session.getInstance(props,
     new javax.mail.Authenticator() {
        protected PasswordAuthentication getPasswordAuthentication() {
           return new PasswordAuthentication(username, password);
        }
     });

  
        // Create a default MimeMessage object.
        Message message = new MimeMessage(session);

        // Set From: header field of the header.
        try {
			message.setFrom(new InternetAddress(from));
		} catch (AddressException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // Set To: header field of the header.
        try {
			message.setRecipients(Message.RecipientType.TO,
			InternetAddress.parse(to));
		} catch (AddressException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // Set Subject: header field
        try {
			message.setSubject("Password Reset Email!!");
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // Now set the actual message
      //  message.setContent("<html><body>Hi,<br/><br/><a href='http://localhost:8080/usermod/ConfirmRegistration/"+token+"/'> Click here</a> to confirm Registration</body></html>", "text/html" );

       
        try {
			message.setContent("<html><body>Hi,<br/><br/>"+"Your new Password is "+token+"</body></html>", "text/html" );
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        
        
        // Send message
        try {
			Transport.send(message);
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        status = "true";

        System.out.println("Sent message successfully....");

       return status;

	
	
	
	
	


}














}  