package edu.virginia.lib.indexing;

import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FedoraRiSearcher {

    public static final String IS_PART_OF = "info:fedora/fedora-system:def/relations-external#isPartOf";
    public static final String FOLLOWS = "http://fedora.lib.virginia.edu/relationships#follows";
    
    public static final String FOLLOWS_PAGE = "http://fedora.lib.virginia.edu/relationships#isFollowingPageOf";
    public static final String IS_CONSTITUENT_OF = "http://fedora.lib.virginia.edu/relationships#isConstituentOf";

    /**
     * Gets the subjects of the given predicate for which the object is give given object.
     * For example, a relationship like "[subject] follows [object]" this method would always
     * return the subject that comes before the given object.
     * @param fc the fedora client that mediates access to fedora
     * @param object the pid of the object that will have the given predicate relationship
     * to all subjects returned.
     * @param predicate the predicate to query
     * @return the URIs of the subjects that are related to the given object by the given
     * predicate
     */

    public static List<String> getSubjects(FedoraClient fc, String object, String predicate) throws Exception {
        if (predicate == null) {
            throw new NullPointerException("predicate must not be null!");
        }
        String itqlQuery = "select $subject from <#ri> where $subject <" + predicate + "> " + (object != null ? "<info:fedora/" + object + ">" : "$other");
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("simple").execute(fc).getEntityInputStream()));
        List<String> pids = new ArrayList<String>();
        String line = null;
        Pattern p = Pattern.compile("\\Qsubject : <info:fedora/\\E([^\\>]+)\\Q>\\E");
        while ((line = reader.readLine()) != null) {
            Matcher m = p.matcher(line);
            if (m.matches()) {
                pids.add(m.group(1));
            }
        }
        return pids;
    }

    /**
     * Gets the objects of the given predicate for which the subject is give given subject.
     * For example, a relationship like "[subject] hasMarc [object]" this method would always
     * return marc record objects for the given subject.
     * @param fc the fedora client that mediates access to fedora
     * @param subject the pid of the subject that will have the given predicate relationship
     * to all objects returned.
     * @param predicate the predicate to query
     * @return the URIs of the objects that are related to the given subject by the given
     * predicate
     */
    public static List<String> getObjects(FedoraClient fc, String subject, String predicate) throws Exception {
        if (predicate == null) {
            throw new NullPointerException("predicate must not be null!");
        }
        String itqlQuery = "select $object from <#ri> where " + (subject != null ? "<info:fedora/" + subject + ">" : "$other") + " <" + predicate + "> $object";
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("simple").execute(fc).getEntityInputStream()));
        List<String> pids = new ArrayList<String>();
        String line = null;
        Pattern pidPattern = Pattern.compile("\\Qobject : <info:fedora/\\E([^\\>]+)\\Q>\\E");
        while ((line = reader.readLine()) != null) {
            Matcher m = pidPattern.matcher(line);
            if (m.matches()) {
                pids.add(m.group(1));
            } else {
                Pattern literalPattern = Pattern.compile("\\Qobject : \"\\E([^\\>]+)\\Q\"\\E");
                m = literalPattern.matcher(line);
                if (m.matches()) {
                    pids.add(m.group(1));
                }
            }
        }
        return pids;
    }

    public static int getNumberOfObjectsWithCmodel(FedoraClient fc, String cmodelPid) throws FedoraClientException, IOException {
        String itqlQuery = "select count("
           + "select $object from <#ri> where $object <info:fedora/fedora-system:def/model#hasModel> <info:fedora/" + cmodelPid + ">"
           + ") from <#ri> where $a $b $c";
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("csv").execute(fc).getEntityInputStream()));
        List<String> pids = new ArrayList<String>();
        reader.readLine();
        return Integer.parseInt(reader.readLine());
    }

    public static String getFirstPart(FedoraClient fc, String parent, String isPartOfPredicate, String followsPredicate) throws Exception {
        String itqlQuery = "select $object from <#ri> where $object <" + isPartOfPredicate + "> <info:fedora/" + parent + "> minus $object <" + followsPredicate + "> $other";
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("simple").execute(fc).getEntityInputStream()));
        List<String> pids = new ArrayList<String>();
        String line = null;
        Pattern p = Pattern.compile("\\Qobject : <info:fedora/\\E([^\\>]+)\\Q>\\E");
        while ((line = reader.readLine()) != null) {
            Matcher m = p.matcher(line);
            if (m.matches()) {
                pids.add(m.group(1));
            }
        }
        if (pids.isEmpty()) {
            return null;
        } else if (pids.size() == 1) {
            return pids.get(0);
        } else {
            throw new RuntimeException("Multiple items are \"first\"! " + pids.get(0) + ", " + pids.get(1) + ")");
        }
    }

    public static String getFirstPartUsingParentsListing(FedoraClient fc, String parent, String hasPartPredicate, String followsPredicate) throws Exception {
        String itqlQuery = "select $object from <#ri> where <info:fedora/" + parent + "> <" + hasPartPredicate + "> $object minus $object <" + followsPredicate + "> $other";
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("simple").execute(fc).getEntityInputStream()));
        List<String> pids = new ArrayList<String>();
        String line = null;
        Pattern p = Pattern.compile("\\Qobject : <info:fedora/\\E([^\\>]+)\\Q>\\E");
        while ((line = reader.readLine()) != null) {
            Matcher m = p.matcher(line);
            if (m.matches()) {
                pids.add(m.group(1));
            }
        }
        if (pids.isEmpty()) {
            return null;
        } else if (pids.size() == 1) {
            return pids.get(0);
        } else {
            throw new RuntimeException("Multiple items are \"first\"! " + pids.get(0) + ", " + pids.get(1) + ")");
        }
    }

    public static String getNextPart(FedoraClient fc, String partPid, String followsPredicate) throws Exception {
        String itqlQuery = "select $object from <#ri> where $object <" + followsPredicate + "> <info:fedora/" + partPid + ">";
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("simple").execute(fc).getEntityInputStream()));
        List<String> pids = new ArrayList<String>();
        String line = null;
        Pattern p = Pattern.compile("\\Qobject : <info:fedora/\\E([^\\>]+)\\Q>\\E");
        while ((line = reader.readLine()) != null) {
            Matcher m = p.matcher(line);
            if (m.matches()) {
                pids.add(m.group(1));
            }
        }
        if (pids.isEmpty()) {
            return null;
        } else if (pids.size() == 1) {
            return pids.get(0);
        } else {
            throw new RuntimeException("Multiple items follow " + partPid + "!");
        }
    }

    public static List<String> getOrderedParts(FedoraClient fc, String parent, String isPartOfPredicate, String followsPredicate) throws Exception {
        String itqlQuery = "select $object $previous from <#ri> where $object <" + isPartOfPredicate + "> <info:fedora/" + parent + "> and $object <" + followsPredicate + "> $previous";
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("csv").execute(fc).getEntityInputStream()));
        Map<String, String> prevToNextMap = new HashMap<String, String>();
        String line = reader.readLine(); // read the csv labels
        Pattern p = Pattern.compile("\\Qinfo:fedora/\\E([^,]*),\\Qinfo:fedora/\\E([^,]*)");
        while ((line = reader.readLine()) != null) {
            Matcher m = p.matcher(line);
            if (m.matches()) {
                prevToNextMap.put(m.group(2), m.group(1));
            } else {
                throw new RuntimeException(line + " does not match pattern!");
            }
        }

        List<String> pids = new ArrayList<String>();
        String pid = getFirstPart(fc, parent, isPartOfPredicate, followsPredicate);
        if (pid == null && !prevToNextMap.isEmpty()) {
            // this is to handle some broke objects... in effect it treats
            // objects whose "previous" is not a sibling as if they had
            // no "previous"
            for (String prev : prevToNextMap.keySet()) {
                if (!prevToNextMap.values().contains(prev)) {
                    if (pid == null) {
                        pid = prev;
                    } else {
                        throw new RuntimeException("Two \"first\" children!");
                    }
                }
            }
        }
        while (pid != null) {
            pids.add(pid);
            String nextPid = prevToNextMap.get(pid);
            prevToNextMap.remove(pid);
            pid = nextPid;

        }
        if (!prevToNextMap.isEmpty()) {
            for (Map.Entry<String, String> entry : prevToNextMap.entrySet()) {
                System.err.println(entry.getKey() + " --> " + entry.getValue());
            }
            throw new RuntimeException("Broken relationship chain");
        }
        return pids;
    }

    public static List<String> getOrderedPartsUsingParentsListing(FedoraClient fc, String parent, String hasPartPredicate, String followsPredicate) throws Exception {
        String itqlQuery = "select $object $previous from <#ri> where <info:fedora/" + parent + "> <" + hasPartPredicate + "> $object and $object <" + followsPredicate + "> $previous";
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("csv").execute(fc).getEntityInputStream()));
        Map<String, String> prevToNextMap = new HashMap<String, String>();
        String line = reader.readLine(); // read the csv labels
        Pattern p = Pattern.compile("\\Qinfo:fedora/\\E([^,]*),\\Qinfo:fedora/\\E([^,]*)");
        while ((line = reader.readLine()) != null) {
            Matcher m = p.matcher(line);
            if (m.matches()) {
                prevToNextMap.put(m.group(2), m.group(1));
            } else {
                throw new RuntimeException(line + " does not match pattern!");
            }
        }

        List<String> pids = new ArrayList<String>();
        String pid = getFirstPartUsingParentsListing(fc, parent, hasPartPredicate, followsPredicate);
        if (pid == null && !prevToNextMap.isEmpty()) {
            // this is to handle some broke objects... in effect it treats
            // objects whose "previous" is not a sibling as if they had
            // no "previous"
            for (Map.Entry<String, String> entry:prevToNextMap.entrySet()) {
                if (!prevToNextMap.values().contains(entry.getKey())) {  // the current item doesn't follow any other items that are parts of the parent
                    if (pid == null) {
                        pid = entry.getValue();
                    } else {
                        throw new RuntimeException("Two \"first\" children!");
                    }
                }
            }
        }

        while (pid != null) {
            pids.add(pid);
            String nextPid = prevToNextMap.get(pid);
            prevToNextMap.remove(pid);
            pid = nextPid;

        }
        if (prevToNextMap.size() > 1 || (prevToNextMap.size() == 1 && !prevToNextMap.entrySet().iterator().next().getValue().equals(pids.get(0)))) {
            for (Map.Entry<String, String> entry : prevToNextMap.entrySet()) {
                System.err.println(entry.getKey() + " --> " + entry.getValue());
            }
            throw new RuntimeException("Broken relationship chain");
        }
        return pids;
    }
}
