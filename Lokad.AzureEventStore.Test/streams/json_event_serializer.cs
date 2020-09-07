using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.Serialization;
using System.Text;
using Lokad.AzureEventStore.Streams;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Lokad.AzureEventStore.Test.streams
{
    public interface ISerializableEvent {}

    [DataContract]
    public class StringEvent : ISerializableEvent
    {
        [DataMember]
        public string String { get; set; }
    }

    [DataContract]
    public class ComplexEvent : ISerializableEvent
    {
        [DataMember(IsRequired = true)]
        public float Real { get; set; }

        [DataMember(IsRequired = true)]
        public float Imaginary { get; set; }
    }

    /// <summary>
    /// An event containing a Dictionary of string --> string
    /// </summary>
    [DataContract]
    public class EventWithDic : ISerializableEvent
    {
        [DataMember]
        public Dictionary<string, string> MyAwesomeDictionary { get; set; }
    }

    public sealed class json_event_serializer
    {
        private JsonEventSerializer<ISerializableEvent> _serializer =
            new JsonEventSerializer<ISerializableEvent>();

        [Fact]
        public void json_of_string()
        {
            var json = _serializer.Serialize(new StringEvent {String = "Hello"});
            Assert.True(json.Length % 8 == 0);
            Assert.Equal(
                "{\"String\":\"Hello\",\"Type\":\"StringEvent\"} ",
                Encoding.ASCII.GetString(json));
        }

        [Fact]
        public void json_of_jobject()
        {
            var serializer = new JsonEventSerializer<JObject>();
            var obj = new JObject {{"String", new JValue("Hello")}, {"Type", new JValue("StringEvent")}};
            var json = serializer.Serialize(obj);
            Assert.True(json.Length % 8 == 0);
            Assert.Equal(
                "{\"String\":\"Hello\",\"Type\":\"StringEvent\"} ",
                Encoding.ASCII.GetString(json));
        }

        [Fact]
        public void json_of_unicode_string()
        {
            var json = _serializer.Serialize(new StringEvent { String = "H€llo" });
            Assert.True(json.Length % 8 == 0);
            Assert.Equal(
                "{\"String\":\"H€llo\",\"Type\":\"StringEvent\"}       ",
                Encoding.UTF8.GetString(json));
        }

        [Fact]
        public void json_of_complex()
        {
            var json = _serializer.Serialize(new ComplexEvent { Real = 0, Imaginary = 1 });
            Assert.True(json.Length % 8 == 0);
            Assert.Equal(
                "{\"Real\":0.0,\"Imaginary\":1.0,\"Type\":\"ComplexEvent\"}      ",
                Encoding.UTF8.GetString(json));
        }

        [Fact]
        public void passthru_string()
        {
            var json = _serializer.Serialize(new StringEvent { String = "Hello" });
            var str = (StringEvent)_serializer.Deserialize(json);
            Assert.Equal("Hello", str.String);
        }

        [Fact]
        public void passthru_jobject()
        {
            var serializer = new JsonEventSerializer<JObject>();
            var obj = new JObject { { "String", new JValue("Hello") }, { "Type", new JValue("StringEvent") } };
            var json = serializer.Serialize(obj);
            var obj2 = serializer.Deserialize(json);
            Assert.Equal("Hello", obj2.Property("String").ToObject<string>());
            Assert.Equal("StringEvent", obj2.Property("Type").ToObject<string>());
        }

        [Fact]
        public void passthru_unicode_string()
        {
            var json = _serializer.Serialize(new StringEvent { String = "H€llo" });
            var str = (StringEvent)_serializer.Deserialize(json);
            Assert.Equal("H€llo", str.String);
        }

        [Fact]
        public void passthru_complex()
        {
            var json = _serializer.Serialize(new ComplexEvent { Real = 0, Imaginary = 1 });
            var c = (ComplexEvent) _serializer.Deserialize(json);
            Assert.Equal(0.0, c.Real);
            Assert.Equal(1.0, c.Imaginary);
        }

        [Fact]
        public void error_unknown_type()
        {
            try
            {
                var json = Encoding.UTF8.GetBytes("{\"Type\":\"Missing\"}"); 
                _serializer.Deserialize(json);
                Assert.True(false);
            }
            catch (Exception e)
            {
                Assert.Equal("Unknown 'Type' property: 'Missing'", e.Message);
            }
        }

        [Fact]
        public void error_bad_object()
        {
            try
            {
                var json = Encoding.UTF8.GetBytes("{\"Type\":\"ComplexEvent\"}");
                _serializer.Deserialize(json);
                Assert.True(false);
            }
            catch (JsonSerializationException e)
            {
                Assert.Equal(
                    "Required property 'Real' not found in JSON. Path '', line 1, position 23.",
                    e.Message);
            }
        }

        [Fact]
        public void error_missing_type()
        {
            try
            {
                var json = Encoding.UTF8.GetBytes("{}");
                _serializer.Deserialize(json);
                Assert.True(false);
            }
            catch (Exception e)
            {
                Assert.Equal("Could not find 'Type' property.", e.Message);
            }
        }

        [Fact]
        public void error_wrong_type()
        {
            try
            { 
                var json = Encoding.UTF8.GetBytes("{\"Type\":true}");
                _serializer.Deserialize(json);
                Assert.True(false);
            }
            catch (Exception e)
            {
                Assert.Equal("Could not find 'Type' property.", e.Message);
            }
        }

        [Fact]
        public void test_with_dates_in_dic()
        {
            var dateAsString = new DateTime(1998, 07, 12).ToString("O", CultureInfo.InvariantCulture);
            const string key = "Foo";
            var evt = new EventWithDic()
            {
                MyAwesomeDictionary = new Dictionary<string, string>
                {{
                    key,
                    dateAsString
                }}
            };
            var serzd = _serializer.Serialize(evt);
            var roundtrip = (EventWithDic)_serializer.Deserialize(serzd);

            Assert.Equal(dateAsString, roundtrip.MyAwesomeDictionary[key]);
        }
    }
}
