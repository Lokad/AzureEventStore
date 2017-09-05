using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.Serialization;
using System.Text;
using Lokad.AzureEventStore.Streams;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

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

    [TestFixture]
    public sealed class json_event_serializer
    {
        private JsonEventSerializer<ISerializableEvent> _serializer;

        [SetUp]
        public void SetUp()
        {
            _serializer = new JsonEventSerializer<ISerializableEvent>();
        }
        
        [Test]
        public void json_of_string()
        {
            var json = _serializer.Serialize(new StringEvent {String = "Hello"});
            Assert.IsTrue(json.Length % 8 == 0);
            Assert.AreEqual(
                "{\"String\":\"Hello\",\"Type\":\"StringEvent\"} ",
                Encoding.ASCII.GetString(json));
        }

        [Test]
        public void json_of_jobject()
        {
            var serializer = new JsonEventSerializer<JObject>();
            var obj = new JObject {{"String", new JValue("Hello")}, {"Type", new JValue("StringEvent")}};
            var json = serializer.Serialize(obj);
            Assert.IsTrue(json.Length % 8 == 0);
            Assert.AreEqual(
                "{\"String\":\"Hello\",\"Type\":\"StringEvent\"} ",
                Encoding.ASCII.GetString(json));
        }

        [Test]
        public void json_of_unicode_string()
        {
            var json = _serializer.Serialize(new StringEvent { String = "H€llo" });
            Assert.IsTrue(json.Length % 8 == 0);
            Assert.AreEqual(
                "{\"String\":\"H€llo\",\"Type\":\"StringEvent\"}       ",
                Encoding.UTF8.GetString(json));
        }

        [Test]
        public void json_of_complex()
        {
            var json = _serializer.Serialize(new ComplexEvent { Real = 0, Imaginary = 1 });
            Assert.IsTrue(json.Length % 8 == 0);
            Assert.AreEqual(
                "{\"Real\":0.0,\"Imaginary\":1.0,\"Type\":\"ComplexEvent\"}      ",
                Encoding.UTF8.GetString(json));
        }

        [Test]
        public void passthru_string()
        {
            var json = _serializer.Serialize(new StringEvent { String = "Hello" });
            var str = (StringEvent)_serializer.Deserialize(json);
            Assert.AreEqual("Hello", str.String);
        }

        [Test]
        public void passthru_jobject()
        {
            var serializer = new JsonEventSerializer<JObject>();
            var obj = new JObject { { "String", new JValue("Hello") }, { "Type", new JValue("StringEvent") } };
            var json = serializer.Serialize(obj);
            var obj2 = serializer.Deserialize(json);
            Assert.AreEqual("Hello", obj2.Property("String").ToObject<string>());
            Assert.AreEqual("StringEvent", obj2.Property("Type").ToObject<string>());
        }

        [Test]
        public void passthru_unicode_string()
        {
            var json = _serializer.Serialize(new StringEvent { String = "H€llo" });
            var str = (StringEvent)_serializer.Deserialize(json);
            Assert.AreEqual("H€llo", str.String);
        }

        [Test]
        public void passthru_complex()
        {
            var json = _serializer.Serialize(new ComplexEvent { Real = 0, Imaginary = 1 });
            var c = (ComplexEvent) _serializer.Deserialize(json);
            Assert.AreEqual(0.0, c.Real);
            Assert.AreEqual(1.0, c.Imaginary);
        }

        [Test, ExpectedException(typeof(Exception), ExpectedMessage = "Unknown 'Type' property: 'Missing'")]
        public void error_unknown_type()
        {
            var json = Encoding.UTF8.GetBytes("{\"Type\":\"Missing\"}");
            Assert.IsNotNull(_serializer.Deserialize(json));            
        }

        [Test, ExpectedException(typeof(JsonSerializationException), ExpectedMessage =
            "Required property 'Real' not found in JSON. Path '', line 1, position 23.")]
        public void error_bad_object()
        {
            var json = Encoding.UTF8.GetBytes("{\"Type\":\"ComplexEvent\"}");
            Assert.IsNotNull(_serializer.Deserialize(json));
        }

        [Test, ExpectedException(typeof(Exception), ExpectedMessage = "Could not find 'Type' property.")]
        public void error_missing_type()
        {
            var json = Encoding.UTF8.GetBytes("{}");
            Assert.IsNotNull(_serializer.Deserialize(json));
        }

        [Test, ExpectedException(typeof(Exception), ExpectedMessage = "Could not find 'Type' property.")]
        public void error_wrong_type()
        {
            var json = Encoding.UTF8.GetBytes("{\"Type\":true}");
            Assert.IsNotNull(_serializer.Deserialize(json));
        }

        [Test]
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

            Assert.AreEqual(dateAsString, roundtrip.MyAwesomeDictionary[key]);
        }
    }
}
