package com.ibm.airlytics.retentiontrackerqueryhandler.polls;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;

public class PollResults {

    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(PollResults.class.getName());

    class PollResult {
        String pollId;
        Map<String, Question> questionsMap;

        PollResult(String pollId) {
            this.pollId = pollId;
            questionsMap = new HashMap<>();
        }

        PollResult add(PollResult otherPr) throws Exception {
            if (!pollId.equals(otherPr.pollId)) {
                logger.error("PollResult add: pollId not identical");
                throw new Exception("Fail to add poll result");
            }

            for (Question otherQ: otherPr.questionsMap.values()) {
                String questionId = otherQ.questionId;
                Question q = questionsMap.get(questionId);
                if (q != null) {
                    q.add(otherQ);
                    questionsMap.put(questionId,q);
                } else {
                    questionsMap.put(questionId,otherQ);
                }
            }
            return  this;
        }

        Question getQuestion(String questionId, Boolean putIfMissing) {
            Question q = questionsMap.get(questionId);
            if (q == null && putIfMissing) {
                q = new Question(questionId);
                questionsMap.put(questionId,q);
            }
            return q;
        }

        void setQuestion(Question q) {
            questionsMap.put(q.questionId,q);
        }
    }

    class Question {
        String questionId;
        Map<String, Integer> predefinedAnswersCountMap;
        Integer openAnswerCount;
        Integer totalResponse;
        boolean pi;

        Question(String questionId) {
            this.questionId = questionId;
            predefinedAnswersCountMap = new HashMap<>();
            openAnswerCount = 0;
            totalResponse = 0;
            pi = false;
        }

        Question add(Question otherQ) throws Exception {
            if (!otherQ.questionId.equals(questionId)) {
                logger.error("PollResult Question add: questionId not identical");
                throw new Exception("Fail to add poll result question");
            }

            for (Map.Entry<String, Integer> entry : otherQ.predefinedAnswersCountMap.entrySet()) {
                String otherAnswerId = entry.getKey();
                Integer otherCount = entry.getValue();

                if (predefinedAnswersCountMap.containsKey(otherAnswerId)) {
                    predefinedAnswersCountMap.put(otherAnswerId, predefinedAnswersCountMap.get(otherAnswerId) + otherCount);
                } else {
                    predefinedAnswersCountMap.put(otherAnswerId, otherCount);
                }
            }

            pi = otherQ.pi;
            openAnswerCount += otherQ.openAnswerCount;
            totalResponse += otherQ.totalResponse;
            return this;
        }

        void setPredefinedAnswerCount(String answerId, Integer count) {
           predefinedAnswersCountMap.put(answerId,count);
        }

        void setOpenAnswerCount(Integer count) {
            openAnswerCount = count;
        }

        void setTotalResponse(Integer totalCount) {
            totalResponse = totalCount;
        }

        void setPI(boolean isPI) { pi = isPI;}
    }

    String productId;
    Map<String, PollResult> pollsResultMap;
    int answersProcessedRows;
    int piAnswersProcessedRows;

    public PollResults(String productId)
    {
        this.productId = productId;
        pollsResultMap = new HashMap<>();
        answersProcessedRows = 0;
        piAnswersProcessedRows = 0;
    }

    public boolean isEmpty() {
        return pollsResultMap.isEmpty();
    }

    public void setPredefinedAnswerCount(String pollId, String questionId, String answerId, Integer count, Integer totalCount, Boolean pi) {
        Question q = getPollResultsQuestion(pollId, questionId, true);
        if (q != null) {
            q.setPredefinedAnswerCount(answerId, count);
            q.setTotalResponse(totalCount);
            q.setPI(pi);
        }
    }

    public void setOpenAnswerCount(String pollId, String questionId, Integer count, Integer totalCount, Boolean pi) {
        Question q = getPollResultsQuestion(pollId, questionId, true);
        if (q != null) {
            q.setOpenAnswerCount(count);
            q.setTotalResponse(totalCount);
            q.setPI(pi);
        }
    }

    Question getPollResultsQuestion(String pollId, String questionId, Boolean putIfMissing) {
        Question q = null;
        PollResult pr = getPoll(pollId, putIfMissing);
        if (pr != null) {
            q = pr.getQuestion(questionId, putIfMissing);
        }
        return q;
   }

   PollResult getPoll(String pollId, Boolean putIfMissing) {
        PollResult pr = pollsResultMap.get(pollId);
        if (pr == null && putIfMissing) {
            pr = new PollResult(pollId);
            pollsResultMap.put(pollId,pr);
        }
        return pr;
    }

    void setPoll(PollResult pr) {
        pollsResultMap.put(pr.pollId,pr);
    }

    public void load(String jsonStr) {
        if (jsonStr == null) {
            return;
        }

        JSONObject jsonObject = new JSONObject(jsonStr);
        answersProcessedRows = jsonObject.getInt("answersProcessedRows");
        piAnswersProcessedRows = jsonObject.getInt("piAnswersProcessedRows");
        productId = jsonObject.getString("productId");
        pollsResultMap = new HashMap<>();

        JSONArray pollsJSONArr = jsonObject.getJSONArray("polls");
        for (int i = 0 ; i < pollsJSONArr.length() ; i++) {
            JSONObject pollJSON = pollsJSONArr.getJSONObject(i);
            String poolId = pollJSON.getString("pollId");
            PollResult pr = new PollResult(poolId);
            JSONArray questionsJSONArr = pollJSON.getJSONArray("questions");
            for (int j = 0 ; j < questionsJSONArr.length() ; j++) {
                JSONObject questionJSON = questionsJSONArr.getJSONObject(j);
                String questionId = questionJSON.getString("questionId");
                Question q = new Question(questionId);
                boolean pi = questionJSON.getBoolean("pi");
                Integer totalResponse = questionJSON.getInt("totalResponses");
                Integer openAnswerCount = questionJSON.getJSONObject("openAnswer").getInt("nominal");
                q.setPI(pi);
                q.setTotalResponse(totalResponse);
                q.setOpenAnswerCount(openAnswerCount);
                JSONArray predefinedAnswersJSONArr = questionJSON.getJSONArray("predefinedAnswers");
                for (int k = 0 ; k < predefinedAnswersJSONArr.length() ; k++) {
                    JSONObject predefinedAnswerJSON = predefinedAnswersJSONArr.getJSONObject(k);
                    String answerId = predefinedAnswerJSON.getString("answerId");
                    Integer answerNum = predefinedAnswerJSON.getInt("nominal");
                    q.setPredefinedAnswerCount(answerId,answerNum);
                }
                pr.setQuestion(q);
            }
            setPoll(pr);
        }
    }
    
    public void sumProcessedRows() {
        answersProcessedRows = 0;
        piAnswersProcessedRows = 0;

        for (PollResult pr : pollsResultMap.values()) {
           for (Question q : pr.questionsMap.values()) {
               if (q.pi) {
                   piAnswersProcessedRows += q.totalResponse;
               } else {
                   answersProcessedRows += q.totalResponse;
               }
           }
        }
    }

    public PollResults add(PollResults other) throws Exception {
        if (!productId.equals(other.productId)) {
            logger.error("PollResults add: productId not identical");
            throw new Exception("Fail to add poll results");
        }

        for (PollResult otherPr: other.pollsResultMap.values()) {
            String pollId = otherPr.pollId;
            PollResult pr = pollsResultMap.get(pollId);
            if (pr != null) {
                pr.add(otherPr);
                pollsResultMap.put(pollId,pr);
            } else {
                pollsResultMap.put(pollId,otherPr);
            }
        }

        answersProcessedRows += other.answersProcessedRows;
        piAnswersProcessedRows += other.piAnswersProcessedRows;
        return this;
    }

    public JSONObject toJSONObject() {
        JSONObject o = new JSONObject();
        o.put("productId", productId);
        JSONArray pollsArray = new JSONArray();
        for (PollResult pr: pollsResultMap.values()) {
            JSONObject poll = new JSONObject();
            poll.put("pollId",pr.pollId);
            JSONArray questionsArr = new JSONArray();
            for (Question q: pr.questionsMap.values()) {
                JSONObject questionJSON = new JSONObject();
                questionJSON.put("questionId",q.questionId);
                questionJSON.put("pi",q.pi);
                questionJSON.put("totalResponses", q.totalResponse);
                JSONObject openAnswerJSON = new JSONObject();
                openAnswerJSON.put("nominal",q.openAnswerCount);
                questionJSON.put("openAnswer",openAnswerJSON);
                JSONArray predefinedAnswersArr = new JSONArray();
                for (Map.Entry<String, Integer> answerCountEntery : q.predefinedAnswersCountMap.entrySet()) {
                    JSONObject answerJSON = new JSONObject();
                    answerJSON.put("answerId", answerCountEntery.getKey());
                    answerJSON.put("nominal", answerCountEntery.getValue());
                    predefinedAnswersArr.put(answerJSON);
                }
                questionJSON.put("predefinedAnswers",predefinedAnswersArr);
                questionsArr.put(questionJSON);
            }
            poll.put("questions",questionsArr);
            pollsArray.put(poll);
        }
        o.put("polls", pollsArray);
        return o;
    }

    public String toJSONString(JSONObject o, boolean includeProcessedRows) {
        if (o == null) {
            o = toJSONObject();
        }
        if (includeProcessedRows) {
            o.put("answersProcessedRows", answersProcessedRows);
            o.put("piAnswersProcessedRows", piAnswersProcessedRows);
        }
        return o.toString();
    }
}
