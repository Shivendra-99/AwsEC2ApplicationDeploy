package com.example.AwsEC2ApplicationDeploy;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfCopy;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;

@RestController
public class ApplicationController {
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    @PostMapping("/processFile")
    public Map<String, Object> processFile(@RequestParam String bucketName, @RequestParam String objectKey) {
        Map<String, Object> response = new HashMap<>();
        try {
            System.out.println("Bucket: " + bucketName);
            System.out.println("Key: " + objectKey);
            List<String> keys = s3Client.listObjectsV2(bucketName, "output/").getObjectSummaries().stream()
                    .map(s3ObjectSummary -> s3ObjectSummary.getKey()).collect(Collectors.toList());

            for (String key : keys) {
                if (key.equals("output/")) {
                    continue;
                }
                System.out.println("Key: " + key);
                String KeyContains = key.split("/")[1];
                String objectKeyContains = objectKey.split("/")[1];

                if (KeyContains.startsWith(objectKeyContains.substring(0, 3))) {
                    return updateThePDFFile(key, bucketName, objectKey, response);
                }
            }
            System.out.println("Starting the download of the file from S3");
            S3Object s3Object = s3Client.getObject(bucketName, objectKey);
            S3ObjectInputStream s3InputStream = s3Object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8));
            String line;
            StringBuilder batchContent = new StringBuilder();
            int batchSize = 100; // Number of lines to batch together
            int lineCount = 0;

            System.out.println("Starting the conversion of the file to PDF");

            // Create a PDF from the text content
            Document document = new Document();
            ByteArrayOutputStream pdfOutputStream = new ByteArrayOutputStream();
            PdfWriter writer = PdfWriter.getInstance(document, pdfOutputStream);
            document.open();
            while ((line = reader.readLine()) != null) {
                batchContent.append(line).append("\n");
                lineCount++;
                if (lineCount >= batchSize) {
                    document.add(new Paragraph(batchContent.toString()));
                    batchContent.setLength(0); // Clear the batch content
                    lineCount = 0;
                }
            }
            // Add any remaining content
            if (batchContent.length() > 0) {
                document.add(new Paragraph(batchContent.toString()));
            }
            reader.close();
            document.close();
            writer.close();

            System.out.println("PDF conversion completed");

            // Define the new object key for the PDF
            String pdfKey = objectKey.replace(".txt", ".pdf");
            pdfKey = pdfKey.replace("input/", "output/");
            System.out.println("Uploading the PDF to S3: " + pdfKey);

            // Upload the PDF back to S3 with content length
            byte[] pdfBytes = pdfOutputStream.toByteArray();
            ByteArrayInputStream pdfInputStream = new ByteArrayInputStream(pdfBytes);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(pdfBytes.length);
            metadata.setContentType("application/pdf");
            s3Client.putObject(bucketName, pdfKey, pdfInputStream, metadata);

            System.out.println("PDF uploaded to S3 successfully");

            response.put("statusCode", 200);
            response.put("body", "File uploaded successfully");

        } catch (IOException | DocumentException e) {
            System.out.println("Error during PDF conversion or upload: " + e.getMessage());
            response.put("statusCode", 500);
            response.put("body", "Error: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("General error: " + e.getMessage());
            response.put("statusCode", 400);
            response.put("body", "Error: " + e.getMessage());
        }
        return response;
    }

    public Map<String, Object> updateThePDFFile(String outObject, String bucketName, String objectKey,
            Map<String, Object> response) {
        try {

            System.out.println("inside update pdf method");

            // Reading the text file content from s3
            S3Object Texts3Objects = s3Client.getObject(bucketName, objectKey);
            S3ObjectInputStream s3InputStreams = Texts3Objects.getObjectContent();
            String TextfileContent = new String(s3InputStreams.readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("Text File content: " + TextfileContent);

            // Downloading and Reading the PDF file content from s3
            S3Object PDFs3Object = s3Client.getObject(bucketName, outObject);
            S3ObjectInputStream s3InputStream = PDFs3Object.getObjectContent();

            // Updating the PDF content
            System.out.println(
                    "Updating pdf content " + new String(s3InputStream.readAllBytes(), StandardCharsets.UTF_8));
            PdfReader pdfReader = new PdfReader(s3InputStream);
            ByteArrayOutputStream pdfOutputStream = new ByteArrayOutputStream();
            Document document = new Document();
            PdfCopy copy = new PdfCopy(document, pdfOutputStream);
            document.open();
            copy.addDocument(pdfReader);
            document.newPage();
            for (String content : TextfileContent.split("\n")) {
                document.add(new Paragraph(content));
            }
            document.close();
            copy.close();
            pdfReader.close();

            System.out.println("PDF content update completed");

            // Extract and log the content of the merged PDF
            PdfReader mergedPdfReader = new PdfReader(new ByteArrayInputStream(pdfOutputStream.toByteArray()));
            StringBuilder pdfContent = new StringBuilder();
            for (int i = 1; i <= mergedPdfReader.getNumberOfPages(); i++) {
                pdfContent.append(PdfTextExtractor.getTextFromPage(mergedPdfReader, i));
            }
            mergedPdfReader.close();
            System.out.println("Merged PDF content: " + pdfContent.toString());

            System.out.println("PDF content update completed and uploading pdf to s3");

            // Uploading the updated PDF to S3
            byte[] pdfBytes = pdfOutputStream.toByteArray();
            ByteArrayInputStream pdfInputStream = new ByteArrayInputStream(pdfBytes);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(pdfBytes.length);
            metadata.setContentType("application/pdf");
            s3Client.putObject(bucketName, outObject, pdfInputStream, metadata);
            response.put("statusCode", 200);
            response.put("body", "File updated and uploaded successfully" + pdfOutputStream.toByteArray().toString());
        } catch (IOException | DocumentException e) {
            System.out.println("Error during PDF conversion or upload from update pdf: " + e.getMessage());
            response.put("statusCode", 500);
            response.put("body", "Error from update pdf : " + e.getMessage());
        } catch (Exception e) {
            System.out.println("General error from update pdf: " + e.getMessage());
            response.put("statusCode", 400);
            response.put("body", "Error from update pdf: " + e.getMessage());
        }
        return response;
    }
    
}
