# Osprey

Osprey is a little java tool.

Osprey makes a way that you want execute something ultimately by persistent staffs in to file.

[Dowload] (https://github.com/downloads/echozhjun/osprey/osprey-1.0.0.jar "download")

# QuikStart

```java
public class OspreyManagerTest {

    public static void main(String[] args) {
//        BasicConfigurator.configure();
        OspreyManager ospreyManager = new OspreyManager("osprey-test");
        ospreyManager.registerProcessor(new TestProcessor());
        ospreyManager.init();
        Message message = new TestMessage();
        Result result = ospreyManager.addMessage(message, false);
        System.out.println(result.isSuccess());
        result = ospreyManager.commitMessage(message, result);
        System.out.println(result.isSuccess());
    }

    public static class TestMessage extends Message {

        private static final long serialVersionUID = -9006052790210673532L;

    }

    public static class TestProcessor implements OspreyProcessor<TestMessage> {

        /**
         * @return
         */
        public Class<TestMessage> interest() {
            return TestMessage.class;
        }

        /**
         * @param event
         * @return
         */
        public Result process(TestMessage event) {
            System.out.println("Handled event" + event);
            Result result = new Result();
            result.setSuccess(false);
            return result;
        }

    }

}
```