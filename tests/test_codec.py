import unittest

from uric.codec import e2ap, e2sm, kpm, rc
from uric.codec.asn import CodecError, float_to_real, real_to_float


class TestE2AP(unittest.TestCase):
    def test_setup_roundtrip(self):
        gid = e2ap.gnb_global_node_id("00101", 0xE00, 32, du_id=3)
        data = e2ap.encode("E2setupRequest", {
            "TransactionID": 7, "GlobalE2node-ID": gid,
            "RANfunctionsAdded": [{"ranFunctionID": 2, "ranFunctionDefinition": b"x",
                                   "ranFunctionRevision": 1, "ranFunctionOID": kpm.OID}],
            "E2nodeComponentConfigAddition": [{
                "e2nodeComponentInterfaceType": "f1",
                "e2nodeComponentID": ("e2nodeComponentInterfaceTypeF1", {"gNB-DU-ID": 3}),
                "e2nodeComponentConfiguration": {"e2nodeComponentRequestPart": b"a",
                                                 "e2nodeComponentResponsePart": b"b"}}]})
        msg = e2ap.decode(data)
        self.assertEqual(msg.name, "E2setupRequest")
        self.assertEqual(msg.procedure_code, 1)
        self.assertEqual(msg["TransactionID"], 7)
        self.assertEqual(msg["RANfunctionsAdded"][0]["ranFunctionID"], 2)
        self.assertEqual(e2ap.global_node_key(msg["GlobalE2node-ID"]), "gnb_00101_e00_du3")

    def test_subscription_request(self):
        data = e2ap.encode("RICsubscriptionRequest", {
            "RICrequestID": e2ap.ric_request_id(5, 9), "RANfunctionID": 2,
            "RICsubscriptionDetails": {"ricEventTriggerDefinition": kpm.event_trigger(500),
                                       "ricAction-ToBeSetup-List": [
                                           {"ricActionID": 1, "ricActionType": "report",
                                            "ricActionDefinition": kpm.action_format1(["A"], 500)}]}})
        msg = e2ap.decode(data)
        det = msg["RICsubscriptionDetails"]
        self.assertEqual(msg["RICrequestID"], {"ricRequestorID": 5, "ricInstanceID": 9})
        self.assertEqual(kpm.decode_event_trigger(det["ricEventTriggerDefinition"])["period_ms"], 500)
        self.assertEqual(det["ricAction-ToBeSetup-List"][0]["ricActionID"], 1)

    def test_unknown_ie_rejected(self):
        with self.assertRaises(CodecError):
            e2ap.encode("ResetRequest", {"TransactionID": 1, "RICrequestID": e2ap.ric_request_id(1, 1)})

    def test_plmn(self):
        for p in ("00101", "310410", "99970"):
            self.assertEqual(e2ap.bytes_to_plmn(e2ap.plmn_to_bytes(p)), p)
        self.assertEqual(e2ap.plmn_to_bytes("00101"), b"\x00\xf1\x10")

    def test_every_procedure_known(self):
        for name in ("E2setupRequest", "E2setupResponse", "RICindication", "RICcontrolRequest",
                     "RICserviceQuery", "ResetRequest", "E2RemovalRequest", "ErrorIndication",
                     "RICsubscriptionDeleteRequired", "E2nodeConfigurationUpdate"):
            self.assertIn(name, e2ap.PROCEDURES)


class TestKPM(unittest.TestCase):
    def test_ran_function(self):
        name = {"ranFunction-ShortName": kpm.SHORT_NAME, "ranFunction-E2SM-OID": kpm.OID,
                "ranFunction-Description": "KPM"}
        caps = kpm.decode_ran_function(kpm.ran_function_description(
            name, [{"type": 1, "name": "node", "metrics": ["A", "B"]}]))
        self.assertEqual(caps["service_model"], "KPM")
        self.assertEqual([m["name"] for m in caps["report_styles"][0]["metrics"]], ["A", "B"])

    def test_action_definitions(self):
        ue = e2sm.gnb_ueid(4)
        self.assertEqual(kpm.decode_action_definition(kpm.action_format1(["A"], 100))["format"], 1)
        self.assertEqual(kpm.decode_action_definition(kpm.action_format2(ue, ["A"]))["ue_id"], ue)
        d = kpm.decode_action_definition(kpm.action_format4([kpm.test_condition("sNSSAI", "equal", 1)], ["A"]))
        self.assertEqual(d["style"], 4)
        self.assertEqual(d["ue_conditions"][0]["testValue"], ("valueInt", 1))
        d = kpm.decode_action_definition(kpm.action_format5([ue, e2sm.gnb_ueid(5)], ["A"]))
        self.assertEqual(len(d["ue_ids"]), 2)

    def test_indications(self):
        hdr = kpm.indication_header(1_700_000_000.5)
        rep = kpm.decode_indication(hdr, kpm.indication_message_format1(["A", "B", "C"], [3, 2.5, None]))
        self.assertAlmostEqual(rep.timestamp, 1_700_000_000.5, places=3)
        self.assertEqual(rep.measurements, {"A": [3], "B": [2.5], "C": [None]})
        ue = e2sm.gnb_ueid(9)
        rep = kpm.decode_indication(hdr, kpm.indication_message_format3([(ue, ["X"], [-101.5])]))
        self.assertEqual(rep.latest("X", "gNB-UEID:9"), -101.5)
        rep = kpm.decode_indication(hdr, kpm.indication_message_format2(
            [("X", [{"matchingCondChoice": ("testCondInfo", kpm.test_condition("rSRP", "present"))}], [ue], 7)]))
        self.assertEqual(rep.measurements["X"], [7])
        self.assertIn("gNB-UEID:9", rep.ues)

    def test_real(self):
        for v in (0.0, 1.0, -2.75, 1e-6, 123456.789):
            self.assertAlmostEqual(real_to_float(float_to_real(v)), v)


class TestRC(unittest.TestCase):
    def test_nested_ran_parameters(self):
        params = rc.slice_prb_quota_params([{"sst": 1, "min": 5, "max": 50}, {"sst": 2, "sd": 7, "max": 60}])
        back = rc.decode_control_message(rc.control_message(params))["params"]
        self.assertEqual(back, params)

    def test_key_and_types(self):
        params = {1: rc.Key(5), 2: True, 3: 1.5, 4: b"\x01", 5: "x", 6: (5, 4), 7: [{8: 1}, {8: 2}]}
        back = rc.decode_control_message(rc.control_message(params))["params"]
        self.assertEqual(back[1], 5)
        self.assertEqual(back[6], (5, 4))
        self.assertEqual(back[7], [{8: 1}, {8: 2}])

    def test_handover(self):
        cgi = e2sm.nr_cgi("00101", 0xE001)
        p = rc.decode_control_message(rc.control_message(rc.handover_params(cgi)))["params"]
        self.assertEqual(rc.parse_handover_target(p), ("nr", "00101", 0xE001))
        h = rc.decode_control_header(rc.control_header(e2sm.gnb_ueid(1), 3, 1))
        self.assertEqual((h["style"], h["action"]), (3, 1))

    def test_ran_function_definition(self):
        name = {"ranFunction-ShortName": rc.SHORT_NAME, "ranFunction-E2SM-OID": rc.OID,
                "ranFunction-Description": "RC"}
        d = rc.ran_function_definition(name, control_styles=[{"type": 3, "name": "mob", "actions": [
            {"id": 1, "name": "HO", "params": [{"id": 1, "name": "a", "definition": {
                "kind": "structure", "params": [{"id": 2, "name": "b"}]}}]}]}])
        caps = rc.decode_ran_function(d)
        self.assertEqual(caps["control_styles"][0]["actions"][0]["params"][0]["definition"]["params"][0]["name"], "b")

    def test_ue_info_indication(self):
        ue, cgi = e2sm.gnb_ueid(3), e2sm.nr_cgi("00101", 1)
        d = rc.decode_indication(rc.indication_header_format1(1), rc.indication_message_format4([(ue, cgi, None)]))
        self.assertEqual(d["ues"]["gNB-UEID:3"]["cell"], cgi)


if __name__ == "__main__":
    unittest.main()
