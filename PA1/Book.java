package ngram;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//IMPORTANT: make sure you understand ALL code and can EXPLAIN it
//IMPORTANT: make sure you understand the role of EACH class
public class Book {
    private String headerText, bodyText, author, year;
    private int ngramCount; // #TODO#: specify unigram or bigram

    public Book(String rawText, int ngramCount) {

       
        this.ngramCount = ngramCount;
        
        String reg = " \\*\\*\\*";

        String[]str = rawText.split(reg , 2);
        headerText = str[0];
        bodyText = str[1];

        bodyText = formatBook(bodyText);
        author = parseAuthor(headerText);
        year = parseYear(headerText);
    }

    private String parseAuthor(String headerText) {
        
        Pattern authorPattern = Pattern.compile("Author: (.*)");
        Matcher authorMatcher = authorPattern.matcher(headerText);
        if(authorMatcher.find()){
            String authorMatch = authorMatcher.group(1);
            String [] p = authorMatch.split(" ");
            authorMatch = p[0];
            return authorMatch;
        }
        return "";
    }

    private String parseYear(String headerText) {
        Pattern yearPattern = Pattern.compile("Release Date: (.*)");
        String yearMatch = "";
        Matcher yearMatcher = yearPattern.matcher(headerText);
        if(yearMatcher.find()){
             yearMatch = yearMatcher.group(1);

        }
        Pattern p = Pattern.compile("([0-9]{4})");
        Matcher m = p.matcher(yearMatch);
        if(m.find()){
            yearMatch = m.group(0);
        }

        return yearMatch;
    }

    public String getBookAuthor() {
        return author;
    }

    public String getBookYear() {
        return year;
    }

    public String getBookHeader() {
        return headerText;
    }

    public String getBookBody() {
        return bodyText;
    }

    private String formatBook(String bookText) {
        String formatted = bookText.trim().replaceAll("[^a-zA-Z\n ]","").toLowerCase();
        StringBuilder sb = new StringBuilder(formatted);
        sb.insert(0,"_");
        sb.insert(sb.length(),"_");
        if(ngramCount < 2)
            //"#TODO# BOOK TEXT FOR UNIGRAM - DON'T FORGET PRE-PROCESSING"
            return formatted;
            //hint: convert text to lower case and remove punctuations and apostrophies
        else
            return sb.toString();
        //hint: all from previous return + add sentence beginning and end tokens
    }
}
