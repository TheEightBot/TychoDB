using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tycho;
using Xamarin.Essentials;
using Xamarin.Forms;

namespace SqliteJson
{
    public partial class MainPage : ContentPage
    {
        public MainPage ()
        {
            InitializeComponent ();
        }

        protected override async void OnAppearing ()
        {
            base.OnAppearing ();

            using var db =
                await new TychoDb (FileSystem.AppDataDirectory)
                    .ConnectAsync();

            var testObj =
                new TestClassA
                {
                    StringProperty = "Test String",
                    IntProperty = 1984,
                    TimestampMillis = 123451234,
                };

            var writeResult = await db.WriteObjectAsync (testObj, x => x.StringProperty);

            var readResult = await db.ReadObjectAsync<TestClassA> (testObj.StringProperty);

            System.Diagnostics.Debug.WriteLine ($"{readResult}");
        }
    }

    class TestClassA
    {
        public string StringProperty { get; set; }

        public int IntProperty { get; set; }

        public long TimestampMillis { get; set; }
    }
}
