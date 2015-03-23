/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unitn.disi.db.queryrelaxation.statistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public final class BenefitGenerator {
    private BenefitGenerator() {}
    private static final String DB_REGEXP = "[\\p{Alnum}_]+BOOLEAN_[0-9]+[\\p{Alnum}_]+.txt";    
    
    public static void addBenefitsToDbDirectory(String directory, boolean zero) throws IOException {
        Pattern p = Pattern.compile(DB_REGEXP);
        List<File> benefitGenerator = Utilities.findFilesOnSubdirs(directory, p);
        for (int i = 0; i < benefitGenerator.size(); i++) {
            generateBenefits(benefitGenerator.get(i).getAbsolutePath(), zero);
        }
    }
    
    public static void generateZeroBenefits(String dbFile) throws IOException {
        File file = new File(dbFile);
        if (file.isDirectory())
            addBenefitsToDbDirectory(dbFile, true);
        else
            generateBenefits(dbFile, true);
    }
    
    public static void generateBenefits(String dbFile) throws IOException {
        File file = new File(dbFile);
        if (file.isDirectory())
            addBenefitsToDbDirectory(dbFile, false);
        else
            generateBenefits(dbFile, false);
    }
    
    /**
     * Generate benefits for the database input file. Put an extra column that 
     * reresents a value for that tuple. In the meanwhile the generator works
     * randomly. 
     * 
     * @param dbFile The file to be modified
     * @param zero Generate 0 benefits
     * @throws IOException 
     */
    public static void generateBenefits(String dbFile, boolean zero) throws IOException {
        BufferedReader reader = null; 
        BufferedWriter writer = null;
        String line = null;
        String[] splittedLine = null;
        List<Double> benefits;
        List<String> lines;
        int sum = 0; 
        File db = new File(dbFile);
        Random initialBenefit = new Random(); 
        
        try {
            reader = new BufferedReader(new FileReader(dbFile));
            benefits = new ArrayList<Double>();
            lines = new ArrayList<String>();
            while ((line = reader.readLine()) != null) {
                splittedLine = line.split(" |\t");
                lines.add(line);
                if (zero) {
                    benefits.add(0.);                    
                } else {
                    sum = initialBenefit.nextInt(splittedLine.length/2) + 1;
                    for (int i = 0; i < splittedLine.length; i++) {
                        sum += Short.parseShort(splittedLine[i]);
                    }
                    benefits.add(sum + (initialBenefit.nextDouble())); 
                }
            }
            if (!zero)  {
                normalizeBenefits(benefits);
            }
            
            reader.close();
            db.delete();
            writer = new BufferedWriter(new FileWriter(dbFile));
            for (int i = 0; i < lines.size(); i++) {
                writer.append(benefits.get(i) + "").append("\t").append(lines.get(i)).append("\n");
            } 
        } catch (IOException ex) {
            throw ex;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ex) {
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception ex) {
                }
            }
        }
    }
    
    public static void normalizeBenefits(List<Double> benefits) {
        double sum = 0.0; 
        for (int i = 0; i < benefits.size(); i++) {
            sum += benefits.get(i);
        }
        for (int i = 0; i < benefits.size(); i++) {
            benefits.set(i, benefits.get(i)/sum);
        }
    }
    
}
