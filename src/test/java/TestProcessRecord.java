
import com.nyble.main.RecordProcessorImpl;
import com.nyble.match.Matcher;
import com.nyble.match.SystemConsumerEntity;
import com.nyble.models.consumer.CAttribute;
import com.nyble.models.consumer.Consumer;
import com.nyble.models.consumer.ConsumerFlag;
import com.nyble.topics.Names;
import com.nyble.topics.consumer.ChangedProperty;
import com.nyble.topics.consumer.ConsumerValue;
import junit.framework.TestCase;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class TestProcessRecord extends TestCase {

    @InjectMocks
    RecordProcessorImpl processor;

    @Mock
    Matcher matcher;


    RecordProcessorImpl processorSpy ;

    public void setUp() throws SQLException, IllegalAccessException {
        MockitoAnnotations.initMocks(this);
        when(matcher.getConsumersEntityId(anyInt(), anyInt(), anyMap())).thenReturn(
//                Collections.singletonList(new SystemConsumerEntity(1, 1, 1))
                Collections.emptyList()
        );
        processor.setMatcher(matcher);
        processorSpy = spy(processor);
        doNothing().when(processorSpy).updateConsumersEntityId(anyList());
    }

    public void test_nullChangedProperty() throws SQLException, IllegalAccessException {

        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute("10", now));
        c.setProperty("fullName", new CAttribute("MARIUSAFTEI", now));
        ConsumerValue cv = new ConsumerValue(c, null);


        processorSpy.processConsumerValue(cv);
        verify(matcher, times(0)).getConsumersEntityId(anyInt(), anyInt(), anyMap());
    }

    public void test_nullChangedPropertyName() throws SQLException, IllegalAccessException {

        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute("10", now));
        c.setProperty("fullName", new CAttribute("MARIUSAFTEI", now));
        ChangedProperty cp = new ChangedProperty(null, "", "MARIUSAFTEI");
        ConsumerValue cv = new ConsumerValue(c, cp);


        processorSpy.processConsumerValue(cv);
        verify(matcher, times(0)).getConsumersEntityId(anyInt(), anyInt(), anyMap());
    }

    public void test_emptyChangedPropertyName() throws SQLException, IllegalAccessException {

        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute("10", now));
        c.setProperty("fullName", new CAttribute("MARIUSAFTEI", now));
        ChangedProperty cp = new ChangedProperty("", "", "MARIUSAFTEI");
        ConsumerValue cv = new ConsumerValue(c, cp);


        processorSpy.processConsumerValue(cv);
        verify(matcher, times(0)).getConsumersEntityId(anyInt(), anyInt(), anyMap());
    }

    public void test_otherChangedPropertyName() throws SQLException, IllegalAccessException {

        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute("10", now));
        c.setProperty("fullName", new CAttribute("MARIUSAFTEI", now));
        ChangedProperty cp = new ChangedProperty("entityId", "", "MARIUSAFTEI");
        ConsumerValue cv = new ConsumerValue(c, cp);


        processorSpy.processConsumerValue(cv);
        verify(matcher, times(0)).getConsumersEntityId(anyInt(), anyInt(), anyMap());
    }

    public void test_processRecord() throws SQLException, IllegalAccessException {

        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute("10", now));
        c.setProperty("fullName", new CAttribute("MARIUSAFTEI", now));
        ChangedProperty cp = new ChangedProperty("fullName", "", "MARIUSAFTEI");
        ConsumerValue cv = new ConsumerValue(c, cp);


        processorSpy.processConsumerValue(cv);
        verify(matcher, times(1)).getConsumersEntityId(anyInt(), anyInt(), anyMap());
    }

    public void test_processRecordPhoneValidChange() throws SQLException, IllegalAccessException {

        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute("10", now));
        c.setProperty("fullName", new CAttribute("MARIUSAFTEI", now));
        c.setProperty("phone", new CAttribute("0745892038", now));
        c.setFlag(ConsumerFlag.IS_PHONE_VALID);
        ChangedProperty cp = new ChangedProperty("flags", "", "MARIUSAFTEI");
        ConsumerValue cv = new ConsumerValue(c, cp);


        processorSpy.processConsumerValue(cv);
        verify(matcher, times(1)).getConsumersEntityId(anyInt(), anyInt(), anyMap());
        verify(processorSpy, times(1)).validationFlagsChanged(anyString(), anyString(), eq(c));
    }
}
